//! Handles sending and recieving messages as packets
//!
//! BcMediaCodex is used with a `[tokio_util::codec::Framed]` to form complete packets
//!
use crate::bcmedia::de::is_bcmedia_magic;
use crate::bcmedia::model::*;
use crate::{Error, Result};
use bytes::{Buf, BytesMut};
use log::*;
use tokio_util::codec::{Decoder, Encoder};

pub struct BcMediaCodex {
	/// If true we will not search for the start of the next packet
	/// in the event that the stream appears to be corrupted
	strict: bool,
	amount_skipped: usize,
}

impl BcMediaCodex {
	pub(crate) fn new(strict: bool) -> Self {
		Self {
			strict,
			amount_skipped: 0,
		}
	}
}

impl Encoder<BcMedia> for BcMediaCodex {
	type Error = Error;

	fn encode(&mut self, item: BcMedia, dst: &mut BytesMut) -> Result<()> {
		let buf: Vec<u8> = Default::default();
		let buf = item.serialize(buf)?;
		dst.extend_from_slice(buf.as_slice());
		Ok(())
	}
}

/// Upper bound on bytes searched for the next magic per resync step, so
/// one corrupt stretch costs bounded work before the decoder yields.
const MAX_RESYNC_SCAN: usize = 1024 * 1024;

/// How many bytes to drop from the front of `buf`, whose leading frame
/// failed to parse: up to the next known magic or, with none in view, all
/// but the last 3 bytes, which may hold the start of a split magic. At
/// least 1 so the decoder always makes progress.
fn resync_offset(buf: &[u8]) -> usize {
	let scan = &buf[..buf.len().min(MAX_RESYNC_SCAN)];
	scan.windows(4)
		.skip(1)
		.position(|w| is_bcmedia_magic(u32::from_le_bytes([w[0], w[1], w[2], w[3]])))
		.map(|p| p + 1)
		.unwrap_or_else(|| scan.len().saturating_sub(3))
		.max(1)
}

impl Decoder for BcMediaCodex {
	type Item = BcMedia;
	type Error = Error;

	/// Since frames can cross EOF boundaries we overload this so it doesn't error if
	/// there are bytes left on the stream
	fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>> {
		match self.decode(buf)? {
			Some(frame) => Ok(Some(frame)),
			None => Ok(None),
		}
	}

	fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
		loop {
			match BcMedia::deserialize(src) {
				Ok(bc) => {
					if self.amount_skipped > 0 {
						trace!("Amount skipped to restore stream: {}", self.amount_skipped);
						self.amount_skipped = 0;
					}
					return Ok(Some(bc));
				}
				Err(Error::NomIncomplete(_)) => {
					if self.amount_skipped > 0 {
						trace!("Amount skipped to restore stream: {}", self.amount_skipped);
						self.amount_skipped = 0;
					}
					return Ok(None);
				}
				Err(e) => {
					if self.strict {
						return Err(e);
					} else if src.is_empty() {
						return Ok(None);
					} else {
						if self.amount_skipped == 0 {
							debug!("Error in stream attempting to restore");
							trace!("   Stream Error: {:?}", e);
						}
						let skip = resync_offset(src);
						self.amount_skipped += skip;
						src.advance(skip);
						continue;
					}
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::bcmedia::model::{BcMedia, BcMediaAac, BcMediaIframe, VideoType};
	use bytes::BytesMut;
	use tokio_util::codec::{Decoder, Encoder};

	fn sample_iframe() -> BcMedia {
		BcMedia::Iframe(BcMediaIframe {
			video_type: VideoType::H264,
			microseconds: 123,
			time: Some(456),
			data: vec![0u8; 32],
		})
	}

	#[test]
	fn encode_writes_serialized_bytes() {
		let mut codec = BcMediaCodex::new(true);
		let mut buf = BytesMut::new();
		codec.encode(sample_iframe(), &mut buf).expect("encode");
		assert!(!buf.is_empty(), "encoder should have produced bytes");
		// Magic at offset 0 should match IFRAME magic (little-endian)
		let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
		assert_eq!(magic, 0x63643030);
	}

	#[test]
	fn encode_then_decode_roundtrips() {
		let mut codec = BcMediaCodex::new(true);
		let mut buf = BytesMut::new();
		codec.encode(sample_iframe(), &mut buf).expect("encode");
		let out = codec.decode(&mut buf).expect("decode ok").expect("some");
		match out {
			BcMedia::Iframe(f) => {
				assert_eq!(f.microseconds, 123);
				assert_eq!(f.time, Some(456));
				assert_eq!(f.data.len(), 32);
			}
			other => panic!("expected Iframe, got {other:?}"),
		}
	}

	#[test]
	fn decode_empty_returns_none() {
		let mut codec = BcMediaCodex::new(true);
		let mut buf = BytesMut::new();
		// Empty buffer → NomIncomplete → None
		let out = codec.decode(&mut buf).expect("decode ok");
		assert!(out.is_none());
	}

	#[test]
	fn decode_incomplete_returns_none() {
		let mut codec = BcMediaCodex::new(true);
		// 2 bytes only — not even a full magic.
		let mut buf = BytesMut::from(&[0x30, 0x30][..]);
		let out = codec.decode(&mut buf).expect("decode ok");
		assert!(out.is_none());
	}

	#[test]
	fn decode_eof_matches_decode_for_complete_frame() {
		let mut codec = BcMediaCodex::new(true);
		let mut buf = BytesMut::new();
		codec
			.encode(BcMedia::Aac(BcMediaAac { data: vec![0u8; 8] }), &mut buf)
			.expect("encode");
		let out = codec.decode_eof(&mut buf).expect("eof ok").expect("some");
		assert!(matches!(out, BcMedia::Aac(_)));
	}

	#[test]
	fn decode_eof_returns_none_for_empty() {
		let mut codec = BcMediaCodex::new(true);
		let mut buf = BytesMut::new();
		let out = codec.decode_eof(&mut buf).expect("eof ok");
		assert!(out.is_none());
	}

	#[test]
	fn decode_strict_bubbles_parse_error() {
		let mut codec = BcMediaCodex::new(true);
		// 4 bytes that don't match any magic → NomError, strict → Err.
		let mut buf = BytesMut::from(&[0xffu8, 0xff, 0xff, 0xff][..]);
		let result = codec.decode(&mut buf);
		assert!(result.is_err(), "expected Err in strict mode");
	}

	#[test]
	fn decode_non_strict_skips_garbage_and_returns_none() {
		let mut codec = BcMediaCodex::new(false);
		// Garbage with no valid magic: non-strict drops it but keeps a
		// 3-byte tail that could start a split magic.
		let mut buf = BytesMut::from(&[0xffu8, 0xff, 0xff, 0xff][..]);
		let out = codec.decode(&mut buf).expect("decode ok");
		assert!(out.is_none());
		assert_eq!(buf.len(), 3);
	}

	#[test]
	fn decode_non_strict_resync_skips_then_parses_next_frame() {
		let mut codec = BcMediaCodex::new(false);
		let mut buf = BytesMut::new();
		// Feed 4 bytes of garbage — decoder drains them silently.
		buf.extend_from_slice(&[0xffu8, 0xff, 0xff, 0xff]);
		let out = codec.decode(&mut buf).expect("decode ok");
		assert!(out.is_none());
		// Now encode a valid frame — decoder should emit it. This
		// exercises the amount_skipped>0 reset branch on the next
		// successful parse.
		codec.encode(sample_iframe(), &mut buf).expect("encode");
		let out = codec.decode(&mut buf).expect("decode ok").expect("some");
		assert!(matches!(out, BcMedia::Iframe(_)));
	}

	#[test]
	fn decode_non_strict_resyncs_to_frame_behind_garbage() {
		// Garbage and a valid frame in one read: only the garbage goes.
		let mut codec = BcMediaCodex::new(false);
		let mut buf = BytesMut::from(&[0x12u8, 0x34, 0x56, 0x78, 0x9a][..]);
		codec.encode(sample_iframe(), &mut buf).expect("encode");
		let out = codec.decode(&mut buf).expect("decode ok").expect("some");
		assert!(matches!(out, BcMedia::Iframe(_)));
		assert!(buf.is_empty());
	}

	#[test]
	fn decode_non_strict_resyncs_across_split_magic() {
		let mut codec = BcMediaCodex::new(false);
		let mut frame = BytesMut::new();
		codec.encode(sample_iframe(), &mut frame).expect("encode");

		let mut buf = BytesMut::from(&[0x12u8; 40][..]);
		buf.extend_from_slice(&frame[..2]);
		assert!(codec.decode(&mut buf).expect("decode ok").is_none());
		buf.extend_from_slice(&frame[2..]);
		let out = codec.decode(&mut buf).expect("decode ok").expect("some");
		assert!(matches!(out, BcMedia::Iframe(_)));
	}

	#[test]
	fn decode_non_strict_amount_skipped_reset_on_incomplete() {
		let mut codec = BcMediaCodex::new(false);
		let mut buf = BytesMut::new();
		// Drain garbage — sets amount_skipped.
		buf.extend_from_slice(&[0xffu8, 0xff, 0xff, 0xff]);
		assert!(codec.decode(&mut buf).expect("decode ok").is_none());
		// Feed a partial magic that looks legal so decode hits
		// NomIncomplete with amount_skipped>0, resetting the counter.
		buf.extend_from_slice(&[0x30u8, 0x30, 0x64, 0x63]); // IFRAME magic LE
		let out = codec.decode(&mut buf).expect("decode ok");
		assert!(out.is_none());
	}
}
