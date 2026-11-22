//! IMA/DVI ADPCM decoder and encoder per IMA Digital Audio specification.
//!
//! Reolink cameras emit (and accept for talk) ADPCM in blocks with a
//! 4-byte predictor header:
//! - 2 bytes little-endian signed i16: initial predictor
//! - 1 byte: initial step index (0–88)
//! - 1 byte: reserved (0)
//!
//! Followed by packed 4-bit nibbles, low nibble first. A block of `n`
//! bytes carries `2 * (n - 4) + 1` samples.
//!
//! [`encode_dvi4`] writes the layout of GStreamer
//! `adpcmenc layout=dvi blockalign=<n>`, which the neolink talk client
//! used: the predictor restarts from the raw header sample at each
//! block, and the step index carries over from the previous block.

const STEP_TABLE: [i32; 89] = [
	7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 19, 21, 23, 25, 28, 31, 34, 37, 41, 45, 50, 55, 60, 66,
	73, 80, 88, 97, 107, 118, 130, 143, 157, 173, 190, 209, 230, 253, 279, 307, 337, 371, 408, 449,
	494, 544, 598, 658, 724, 796, 876, 963, 1060, 1166, 1282, 1411, 1552, 1707, 1878, 2066, 2272,
	2499, 2749, 3024, 3327, 3660, 4026, 4428, 4871, 5358, 5894, 6484, 7132, 7845, 8630, 9493,
	10442, 11487, 12635, 13899, 15289, 16818, 18500, 20350, 22385, 24623, 27086, 29794, 32767,
];

const INDEX_TABLE: [i32; 16] = [-1, -1, -1, -1, 2, 4, 6, 8, -1, -1, -1, -1, 2, 4, 6, 8];

/// IMA ADPCM decoder.
///
/// Holds the running `predictor` sample and `step_index` across nibble
/// calls within a single block. [`decode_block`] reseeds both fields
/// from the 4-byte block header on entry, so the across-block state is
/// not load-bearing — the production call site at
/// `src/stream_source.rs::handle_adpcm` constructs a fresh decoder per
/// packet. [`decode_nibble`] is exposed for incremental decoding within
/// a block; callers wanting cross-block continuity must avoid
/// [`decode_block`] (which always resets) and feed nibbles directly.
///
/// [`decode_block`]: AdpcmDecoder::decode_block
/// [`decode_nibble`]: AdpcmDecoder::decode_nibble
pub struct AdpcmDecoder {
	predictor: i32,
	step_index: i32,
}

impl AdpcmDecoder {
	/// Create a fresh decoder with zeroed predictor and step index.
	pub fn new() -> Self {
		Self {
			predictor: 0,
			step_index: 0,
		}
	}

	/// Decode a whole block (4-byte header + ≥ 1 data byte) to PCM16.
	///
	/// Returns one PCM16 sample per nibble, plus the initial predictor sample
	/// from the header (IMA convention: first sample is the header predictor).
	pub fn decode_block(&mut self, block: &[u8]) -> Result<Vec<i16>, AdpcmError> {
		let mut out = Vec::new();
		self.decode_block_into(block, &mut out)?;
		Ok(out)
	}

	/// Like [`decode_block`] but writes into a caller-owned `Vec` so a
	/// reused buffer skips per-packet allocation. Clears `out` on entry,
	/// reserves enough capacity for the full output, and grows it
	/// in-place. Returns `Err` without touching `out` only if the header
	/// validation fails.
	///
	/// [`decode_block`]: AdpcmDecoder::decode_block
	pub fn decode_block_into(
		&mut self,
		block: &[u8],
		out: &mut Vec<i16>,
	) -> Result<(), AdpcmError> {
		// 4 header bytes + at least 1 data byte. A header-only block
		// would yield a single-sample output of the predictor, which is
		// valid IMA but useless on the wire and just churns the pipeline.
		if block.len() < 5 {
			return Err(AdpcmError::TooShort);
		}
		self.predictor = i16::from_le_bytes([block[0], block[1]]) as i32;
		self.step_index = block[2] as i32;
		if !(0..=88).contains(&self.step_index) {
			return Err(AdpcmError::InvalidStepIndex(block[2]));
		}
		// Reserved byte block[3] ignored.

		let data = &block[4..];
		out.clear();
		out.reserve(1 + data.len() * 2);
		out.push(self.predictor as i16);

		for &byte in data {
			let low = byte & 0x0F;
			let high = (byte >> 4) & 0x0F;
			out.push(self.decode_nibble(low));
			out.push(self.decode_nibble(high));
		}
		Ok(())
	}

	/// Decode a single nibble incrementally, updating internal state.
	///
	/// Only the low 4 bits of `nibble` are used; the upper bits are
	/// masked off to match the IMA wire format and keep
	/// `INDEX_TABLE[nibble as usize]` in bounds.
	pub fn decode_nibble(&mut self, nibble: u8) -> i16 {
		let nibble = nibble & 0x0F;
		let step = STEP_TABLE[self.step_index as usize];
		let sign = nibble & 0x08;
		let magnitude = (nibble & 0x07) as i32;

		let mut diff = step >> 3;
		if magnitude & 4 != 0 {
			diff += step;
		}
		if magnitude & 2 != 0 {
			diff += step >> 1;
		}
		if magnitude & 1 != 0 {
			diff += step >> 2;
		}
		if sign != 0 {
			self.predictor -= diff;
		} else {
			self.predictor += diff;
		}
		self.predictor = self.predictor.clamp(i16::MIN as i32, i16::MAX as i32);

		self.step_index = (self.step_index + INDEX_TABLE[nibble as usize]).clamp(0, 88);

		self.predictor as i16
	}

	/// Reset decoder state (use on desync recovery). Test-only —
	/// production decoders are constructed fresh per packet (see
	/// `src/stream_source.rs::handle_adpcm`), so reset isn't invoked
	/// outside the test suite. Promote to `pub(crate)` if a future
	/// caller wants in-place reuse.
	#[cfg(test)]
	pub(crate) fn reset(&mut self) {
		self.predictor = 0;
		self.step_index = 0;
	}
}

impl Default for AdpcmDecoder {
	fn default() -> Self {
		Self::new()
	}
}

/// Size of the mono block header in bytes.
pub const BLOCK_HEADER_LEN: usize = 4;

/// Number of PCM samples one mono block of `block_align` bytes carries:
/// the header sample plus two per data byte.
pub fn samples_per_block(block_align: usize) -> usize {
	(block_align - BLOCK_HEADER_LEN) * 2 + 1
}

/// Encode one sample to a 4-bit code, then advance `dec` with that code
/// exactly as a decoder will, so encoder and decoder stay in step.
fn encode_sample(dec: &mut AdpcmDecoder, sample: i16) -> u8 {
	let step = STEP_TABLE[dec.step_index as usize];
	let mut diff = i32::from(sample) - dec.predictor;
	let mut code: u8 = 0;
	if diff < 0 {
		code = 8;
		diff = -diff;
	}
	let mut s = step;
	let mut mask: u8 = 4;
	while mask != 0 {
		if diff >= s {
			code |= mask;
			diff -= s;
		}
		s >>= 1;
		mask >>= 1;
	}
	dec.decode_nibble(code);
	code
}

/// Encode mono 16-bit PCM into DVI4 IMA ADPCM blocks of `block_align`
/// bytes each. The last block is padded with silence, so the output
/// length is always a multiple of `block_align`. Empty input gives an
/// empty output.
///
/// # Panics
///
/// Panics if `block_align` is not larger than the 4-byte header.
pub fn encode_dvi4(pcm: &[i16], block_align: usize) -> Vec<u8> {
	assert!(
		block_align > BLOCK_HEADER_LEN,
		"block_align must exceed the {BLOCK_HEADER_LEN}-byte header"
	);
	let spb = samples_per_block(block_align);
	let mut out = Vec::with_capacity(pcm.len().div_ceil(spb) * block_align);
	let mut step_index = 0i32;
	let mut padded = vec![0i16; spb];
	for chunk in pcm.chunks(spb) {
		let block: &[i16] = if chunk.len() == spb {
			chunk
		} else {
			padded[..chunk.len()].copy_from_slice(chunk);
			padded[chunk.len()..].fill(0);
			&padded
		};
		let first = block[0];
		out.extend_from_slice(&first.to_le_bytes());
		out.push(step_index as u8);
		out.push(0);
		let mut dec = AdpcmDecoder {
			predictor: i32::from(first),
			step_index,
		};
		for [a, b] in block[1..].as_chunks::<2>().0 {
			let lo = encode_sample(&mut dec, *a);
			let hi = encode_sample(&mut dec, *b);
			out.push(lo | (hi << 4));
		}
		step_index = dec.step_index;
	}
	out
}

/// Errors returned by [`AdpcmDecoder::decode_block`].
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum AdpcmError {
	/// Block is shorter than the mandatory 4-byte header.
	#[error("block too short: must be at least 4 header bytes")]
	TooShort,
	/// Header step index is outside the valid 0..=88 range.
	#[error("invalid step index {0}; must be 0..=88")]
	InvalidStepIndex(u8),
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn single_nibble_zero_is_silence_ish() {
		let mut d = AdpcmDecoder::new();
		// Starting predictor=0, step_index=0, step=7
		// Nibble 0: diff = 7>>3=0, no additions. Predictor stays 0.
		assert_eq!(d.decode_nibble(0), 0);
	}

	#[test]
	fn decode_block_recovers_header_predictor() {
		let mut block = vec![0x10, 0x00, 5, 0]; // predictor=16, step_index=5, one zero-nibble byte
		block.push(0x00);
		let mut d = AdpcmDecoder::new();
		let samples = d.decode_block(&block).unwrap();
		assert_eq!(samples[0], 16);
		assert_eq!(samples.len(), 3);
	}

	#[test]
	fn rejects_short_block() {
		let mut d = AdpcmDecoder::new();
		assert_eq!(d.decode_block(&[0, 0, 0]), Err(AdpcmError::TooShort));
		// Header-only (4 bytes, no data) is also rejected — would
		// otherwise yield a single-sample useless block.
		assert_eq!(d.decode_block(&[0, 0, 0, 0]), Err(AdpcmError::TooShort));
	}

	#[test]
	fn rejects_invalid_step_index() {
		let mut d = AdpcmDecoder::new();
		assert_eq!(
			d.decode_block(&[0, 0, 90, 0, 0]),
			Err(AdpcmError::InvalidStepIndex(90))
		);
	}

	#[test]
	fn step_index_clamps_at_88() {
		let mut d = AdpcmDecoder::new();
		d.step_index = 88;
		// Nibble 7 adds +8 to index, should clamp to 88
		d.decode_nibble(7);
		assert_eq!(d.step_index, 88);
	}

	#[test]
	fn step_index_floors_at_zero() {
		let mut d = AdpcmDecoder::new();
		d.step_index = 0;
		// Nibble 0 adds -1 to index, should floor at 0
		d.decode_nibble(0);
		assert_eq!(d.step_index, 0);
	}

	#[test]
	fn known_sequence_matches_reference() {
		// A short ramp-up sequence that a reference IMA decoder also produces.
		// Block: predictor=0, step_index=0; nibbles: 4, 4, 4 → three ascending samples.
		let block = &[0, 0, 0, 0, 0x44, 0x04]; // nibbles 4, 4, 4, 0
		let mut d = AdpcmDecoder::new();
		let samples = d.decode_block(block).unwrap();
		// First sample is predictor=0, then three rising, then one more.
		assert_eq!(samples[0], 0);
		assert!(samples[1] > 0);
		assert!(samples[2] > samples[1]);
		assert!(samples[3] > samples[2]);
	}

	#[test]
	fn negative_nibble_decreases_predictor() {
		// The high (sign) bit of a nibble inverts the diff direction.
		// Start with a non-zero predictor so the decrease is observable.
		let mut d = AdpcmDecoder::new();
		d.predictor = 1000;
		d.step_index = 10;
		let before = d.predictor;
		// Nibble 0x0C = sign set + magnitude 4 → predictor -= (step + step>>3)
		d.decode_nibble(0x0C);
		assert!(
			d.predictor < before,
			"sign-set nibble must decrease predictor ({} → {})",
			before,
			d.predictor
		);
	}

	#[test]
	fn reset_clears_predictor_and_step_index() {
		let mut d = AdpcmDecoder::new();
		d.predictor = 1234;
		d.step_index = 42;
		d.reset();
		assert_eq!(d.predictor, 0);
		assert_eq!(d.step_index, 0);
	}

	#[test]
	fn default_constructs_fresh_decoder() {
		let d = AdpcmDecoder::default();
		assert_eq!(d.predictor, 0);
		assert_eq!(d.step_index, 0);
	}

	#[test]
	fn decode_block_into_reuses_buffer_and_clears_old_data() {
		let block = &[0x10, 0x00, 5, 0, 0x44, 0x04];
		let mut buf = vec![999i16, 999, 999, 999, 999];
		let mut d = AdpcmDecoder::new();
		d.decode_block_into(block, &mut buf).unwrap();
		// Old contents replaced; first sample is the header predictor.
		assert_eq!(buf[0], 16);
		assert_eq!(buf.len(), 5);
		// Reusing the same buffer with a new header overwrites cleanly.
		let block2 = &[0x20, 0x00, 7, 0, 0x00];
		d.decode_block_into(block2, &mut buf).unwrap();
		assert_eq!(buf[0], 32);
		assert_eq!(buf.len(), 3);
	}

	#[test]
	fn decode_block_into_does_not_clobber_buffer_on_header_error() {
		let mut buf = vec![1i16, 2, 3];
		let mut d = AdpcmDecoder::new();
		assert_eq!(
			d.decode_block_into(&[0, 0, 90, 0, 0], &mut buf),
			Err(AdpcmError::InvalidStepIndex(90))
		);
		assert_eq!(buf, vec![1i16, 2, 3]);
	}

	fn sine(n: usize, freq: f64, rate: f64, amp: f64) -> Vec<i16> {
		(0..n)
			.map(|i| (amp * (2.0 * std::f64::consts::PI * freq * i as f64 / rate).sin()) as i16)
			.collect()
	}

	#[test]
	fn encode_output_is_whole_blocks_and_pads_the_tail() {
		let ba = 516; // Reolink default: 1025 samples per block
		assert_eq!(samples_per_block(ba), 1025);
		assert!(encode_dvi4(&[], ba).is_empty());
		assert_eq!(encode_dvi4(&[1; 1], ba).len(), ba);
		assert_eq!(encode_dvi4(&[1; 1025], ba).len(), ba);
		assert_eq!(encode_dvi4(&[1; 1026], ba).len(), 2 * ba);
	}

	#[test]
	fn encode_first_code_is_low_nibble() {
		// Header sample 0, then a large positive step, then back to 0:
		// the low nibble holds the max positive code, the high nibble a
		// negative one.
		let ba = 8; // 4 header bytes + 4 data bytes = 9 samples
		let mut pcm = vec![0i16; samples_per_block(ba)];
		pcm[1] = 30000;
		let enc = encode_dvi4(&pcm, ba);
		assert_eq!(enc.len(), ba);
		assert_eq!(enc[4] & 0x0f, 0x7);
		assert_eq!((enc[4] >> 4) & 8, 8);
	}

	/// Encode a sine, decode it block by block with the production
	/// decoder: each header holds the raw first sample and the step
	/// index the previous block ended on, the padded tail decodes to
	/// near silence, and the signal survives with a small error.
	#[test]
	fn encode_round_trips_through_decoder() {
		let ba = 516;
		let spb = samples_per_block(ba);
		let pcm = sine(4 * spb - 300, 440.0, 16000.0, 16000.0);
		let enc = encode_dvi4(&pcm, ba);
		assert_eq!(enc.len(), 4 * ba);

		let mut dec = Vec::new();
		let mut d = AdpcmDecoder::new();
		let mut end_step = 0;
		for (b, block) in enc.chunks_exact(ba).enumerate() {
			assert_eq!(i16::from_le_bytes([block[0], block[1]]), pcm[b * spb]);
			assert_eq!(i32::from(block[2]), end_step, "block {b} step index");
			assert_eq!(block[3], 0);
			dec.extend(d.decode_block(block).unwrap());
			end_step = d.step_index;
		}
		assert!(
			enc[ba + 2] > 0,
			"a loud sine must raise the carried step index"
		);
		assert_eq!(dec.len(), 4 * spb);

		let tail = &dec[pcm.len() + 200..];
		assert!(
			tail.iter().all(|s| s.unsigned_abs() < 64),
			"tail not silent"
		);

		// Skip the first 64 samples while the step size adapts.
		let (mut err2, mut sig2) = (0f64, 0f64);
		for (&a, &b) in pcm.iter().zip(&dec).skip(64) {
			err2 += (f64::from(a) - f64::from(b)).powi(2);
			sig2 += f64::from(a).powi(2);
		}
		let snr_db = 10.0 * (sig2 / err2).log10();
		assert!(snr_db > 25.0, "SNR too low: {snr_db:.1} dB");
	}

	#[test]
	#[should_panic(expected = "block_align must exceed")]
	fn encode_rejects_header_only_block() {
		let _ = encode_dvi4(&[0; 4], 4);
	}

	// Property tests: decode_block must reject any block that fails
	// the size / step-index validation, and never panic on arbitrary
	// input. decode_nibble's contract is "any 4-bit value yields an
	// i16 sample without panicking".
	use proptest::prelude::*;

	proptest! {
		#![proptest_config(ProptestConfig {
			cases: 4096,
			..ProptestConfig::default()
		})]

		/// `decode_block` must:
		/// - never panic on arbitrary bytes (truncated, malformed step);
		/// - on `Ok`, produce exactly `1 + 2 * (block.len() - 4)` samples
		///   (header predictor + two nibbles per data byte);
		/// - on `Ok`, every output sample stays inside `i16` range
		///   (trivially true since the return type is `i16`, but the
		///   property pins down the predictor-clamp invariant);
		/// - be deterministic for a given input.
		#[test]
		fn decode_block_property_invariants(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
			let mut d = AdpcmDecoder::new();
			let result = d.decode_block(&bytes);
			if let Ok(samples) = &result {
				prop_assert_eq!(samples.len(), 1 + 2 * (bytes.len() - 4));
			}
			// Determinism: a second decoder must produce the same output.
			let mut d2 = AdpcmDecoder::new();
			let result2 = d2.decode_block(&bytes);
			prop_assert_eq!(result, result2);
		}

		/// `decode_nibble` must accept the full 0..=255 byte range
		/// without panicking; the function masks to 4 bits internally so
		/// `INDEX_TABLE` and `STEP_TABLE` reads stay in bounds.
		#[test]
		fn decode_nibble_accepts_full_byte_range(byte in any::<u8>()) {
			let mut d = AdpcmDecoder::new();
			let _ = d.decode_nibble(byte);
		}
	}
}
