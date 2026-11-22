//! Last-frame buffer for RTSP placeholder and MQTT preview.

use bytes::Bytes;
use std::sync::RwLock;

use crate::codec::VideoCodec;

/// The most recent keyframe, replayed to a newly attached RTSP client so
/// it can start decoding without waiting for the camera's next IDR.
///
/// Parameter sets are not stored: clients get them out-of-band via the
/// SDP `sprop-*` fmtp attributes.
#[derive(Debug, Clone)]
pub struct VideoBurst {
	/// Codec used for the NAL units in this burst.
	pub codec: VideoCodec,
	/// NAL units that make up the keyframe (I-frame).
	pub iframe_nals: Vec<Vec<u8>>,
	/// The camera's 90 kHz RTP timestamp at the moment this burst's
	/// I-frame was captured. Used by the session send loop to replay
	/// the burst with a timestamp continuous with the live stream
	/// instead of a hardcoded `0` — without this, a new client sees
	/// a 500+ packet cached burst at ts=0 followed by live packets at
	/// ts=millions-of-ticks, and any downstream re-muxer (HA's go2rtc
	/// `ffmpeg:` wrap in particular) breaks on the timestamp jump.
	pub captured_pts_90khz: u32,
}

/// Combined buffer per camera: video burst for RTSP, JPEG for MQTT.
///
/// Uses [`std::sync::RwLock`] (not the tokio variant) so both blocking and
/// async call sites can read cheaply without awaiting. Writes are rare
/// (one per keyframe/snapshot), reads are frequent (one per attached RTSP
/// client), making the read/write asymmetry a good fit.
pub struct LastFrameBuffer {
	video: RwLock<Option<VideoBurst>>,
	jpeg: RwLock<Option<Bytes>>,
}

impl LastFrameBuffer {
	/// Create an empty buffer with no video burst and no JPEG.
	pub fn new() -> Self {
		Self {
			video: RwLock::new(None),
			jpeg: RwLock::new(None),
		}
	}

	/// Replace the current video burst with a new one (typically on keyframe).
	pub fn replace_video(&self, burst: VideoBurst) {
		*self.video.write().expect("video lock poisoned") = Some(burst);
	}

	/// Return a clone of the current video burst, or `None` if none captured yet.
	pub fn video_snapshot(&self) -> Option<VideoBurst> {
		self.video.read().expect("video lock poisoned").clone()
	}

	/// Return `true` if a video burst has been captured.
	pub fn has_video(&self) -> bool {
		self.video.read().expect("video lock poisoned").is_some()
	}

	/// Replace the stored JPEG preview with a new one.
	pub fn set_jpeg(&self, bytes: Bytes) {
		*self.jpeg.write().expect("jpeg lock poisoned") = Some(bytes);
	}

	/// Return a clone of the stored JPEG, or `None` if none captured yet.
	pub fn jpeg(&self) -> Option<Bytes> {
		self.jpeg.read().expect("jpeg lock poisoned").clone()
	}
}

impl Default for LastFrameBuffer {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::Arc;
	use std::thread;

	fn sample_burst() -> VideoBurst {
		VideoBurst {
			codec: VideoCodec::H264,
			iframe_nals: vec![vec![0x65, 0xaa]],
			captured_pts_90khz: 0,
		}
	}

	#[test]
	fn empty_buffer_has_no_video_or_jpeg() {
		let b = LastFrameBuffer::new();
		assert!(!b.has_video());
		assert!(b.jpeg().is_none());
	}

	#[test]
	fn replace_then_snapshot_returns_burst() {
		let b = LastFrameBuffer::new();
		b.replace_video(sample_burst());
		let s = b.video_snapshot().unwrap();
		assert_eq!(s.codec, VideoCodec::H264);
	}

	#[test]
	fn jpeg_round_trip() {
		let b = LastFrameBuffer::new();
		b.set_jpeg(Bytes::from_static(b"\xFF\xD8\xFF"));
		assert_eq!(b.jpeg().unwrap().len(), 3);
	}

	#[test]
	fn default_constructs_empty_buffer() {
		let b = LastFrameBuffer::default();
		assert!(!b.has_video());
		assert!(b.jpeg().is_none());
	}

	#[test]
	fn concurrent_readers_and_writers() {
		let b = Arc::new(LastFrameBuffer::new());
		let mut handles = vec![];

		for _ in 0..4 {
			let b = Arc::clone(&b);
			handles.push(thread::spawn(move || {
				for _ in 0..500 {
					b.replace_video(sample_burst());
				}
			}));
		}
		for _ in 0..8 {
			let b = Arc::clone(&b);
			handles.push(thread::spawn(move || {
				for _ in 0..500 {
					let _ = b.video_snapshot();
				}
			}));
		}
		for h in handles {
			h.join().unwrap();
		}
		assert!(b.has_video());
	}
}
