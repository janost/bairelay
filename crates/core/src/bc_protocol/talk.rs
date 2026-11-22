use super::{BcCamera, Error, Result};
use crate::{bc::model::*, bc::xml::*, bcmedia::model::*};

impl BcCamera {
	///
	/// Finish Talk
	///
	/// The send the talk finish to the camera.
	///
	/// It is also sent when the request for talk config returns status code 422
	///
	/// Not gated on `two_way_audio` (neolink parity): battery doorbells
	/// accept talk without advertising it, so the caller decides.
	///
	pub async fn talk_stop(&self) -> Result<()> {
		let connection = self.get_connection();

		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(MSG_ID_TALKRESET, msg_num).await?;

		let msg = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_TALKRESET,
				channel_id: self.channel_id,
				msg_num,
				stream_type: 0,
				response_code: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg {
				extension: Some(Extension {
					channel_id: Some(self.channel_id),
					..Default::default()
				}),
				payload: None,
			}),
		};

		sub.send(msg).await?;
		let msg = sub.recv().await?;

		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
		} else {
			return Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "The camera did not accept the talk stop command.",
			});
		}

		Ok(())
	}

	///
	/// Requests the [`TalkAbility`] xml
	///
	pub async fn talk_ability(&self) -> Result<TalkAbility> {
		// Querying TalkAbility on a non-talk camera should surface
		// MissingAbility, not the generic protocol error the camera
		// replies with.
		self.has_ability_ro("two_way_audio").await?;
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_get = connection.subscribe(MSG_ID_TALKABILITY, msg_num).await?;
		let get = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_TALKABILITY,
				channel_id: self.channel_id,
				msg_num,
				response_code: 0,
				stream_type: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg {
				extension: Some(Extension {
					channel_id: Some(self.channel_id),
					..Default::default()
				}),
				payload: None,
			}),
		};

		sub_get.send(get).await?;
		let msg = sub_get.recv().await?;

		if let BcBody::ModernMsg(ModernMsg {
			payload:
				Some(BcPayloads::BcXml(BcXml {
					talk_ability: Some(talk_ability),
					..
				})),
			..
		}) = msg.body
		{
			Ok(talk_ability)
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "Expected TalkAbility xml but it was not received",
			})
		}
	}

	///
	/// Send sound to the camera
	///
	/// The data should be in the format as described in `<TalkAbility>` xml
	/// This method assumes that you have set up the data in the desired format
	/// in the `<TalkAbility>` xml
	///
	/// It also checks that it is ADPCM as the code is written to accept only that
	///
	/// # Parameters
	///
	/// * `adpcm` - Data must be adpcm in DVI-4 format
	///
	/// * `talk_config` - The talk config that describes the adpcm data
	///
	/// Not gated on `two_way_audio`, like [`talk_stop`](Self::talk_stop).
	///
	pub async fn talk(&self, adpcm: &[u8], talk_config: TalkConfig) -> Result<()> {
		let connection = self.get_connection();

		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(MSG_ID_TALKCONFIG, msg_num).await?;

		if &talk_config.audio_config.audio_type != "adpcm" {
			return Err(Error::UnknownTalkEncoding);
		}

		let block_size = talk_config.audio_config.length_per_encoder / 2;
		let sample_rate = talk_config.audio_config.sample_rate;
		// The pacing below divides by it; 0 would panic in
		// `Duration::from_secs_f32(inf)`.
		if sample_rate == 0 {
			return Err(Error::Other("talk sample_rate must be greater than 0"));
		}

		let build_talk_config_msg = |talk_config: TalkConfig| Bc {
			meta: BcMeta {
				msg_id: MSG_ID_TALKCONFIG,
				channel_id: self.channel_id,
				msg_num,
				stream_type: 0,
				response_code: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg {
				extension: Some(Extension {
					channel_id: Some(self.channel_id),
					..Default::default()
				}),
				payload: Some(BcPayloads::BcXml(BcXml {
					talk_config: Some(talk_config),
					..Default::default()
				})),
			}),
		};

		sub.send(build_talk_config_msg(talk_config.clone())).await?;
		let mut msg = sub.recv().await?;

		// If another client is already talking OR if we crashed before sending
		// msgid 11 the camera will reply 422. The official client retries
		// the original TalkConfig request — re-send the request, NOT the
		// 422 reply.
		if let BcMeta {
			response_code: 422, ..
		} = msg.meta
		{
			self.talk_stop().await?;
			sub.send(build_talk_config_msg(talk_config)).await?;
			msg = sub.recv().await?;
		}

		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
		} else {
			return Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "The camera did not accept the TalkConfig xml. Audio format is likely incorrect",
			});
		}

		let full_block_size = block_size + 4; // Block size + predictor state
		let msg_num = self.new_message_num();
		let sub = connection.subscribe(MSG_ID_TALK, msg_num).await?;

		const BLOCK_PER_PAYLOAD: usize = 4;
		const BLOCK_HEADER_SIZE: usize = 4;
		const SAMPLES_PER_BYTE: usize = 2;

		for payload_bytes in adpcm.chunks(full_block_size as usize * BLOCK_PER_PAYLOAD) {
			let mut payload = vec![];
			for bytes in payload_bytes.chunks(full_block_size as usize) {
				let bcmedia_adpcm = BcMedia::Adpcm(BcMediaAdpcm {
					data: bytes.to_vec(),
				});
				payload = bcmedia_adpcm.serialize(payload)?;
			}

			let msg = Bc {
				meta: BcMeta {
					msg_id: MSG_ID_TALK,
					channel_id: self.channel_id,
					msg_num,
					stream_type: 0,
					response_code: 0,
					class: 0x6414,
				},
				body: BcBody::ModernMsg(ModernMsg {
					extension: Some(Extension {
						channel_id: Some(self.channel_id),
						binary_data: Some(1),
						..Default::default()
					}),
					payload: Some(BcPayloads::Binary(payload)),
				}),
			};

			sub.send(msg).await?;

			let adpcm_len = payload_bytes.len();
			// There are two samples per byte
			//
			// To calculate the bytes we subtract the block headers from the len
			//
			// There is 1 initial sample stored in the block header so we add that in the end
			//
			// Count the blocks actually in this payload: the last payload
			// can hold fewer than BLOCK_PER_PAYLOAD (or a short block), and
			// a fixed count underflows on it.
			let blocks_in_payload = adpcm_len.div_ceil(full_block_size as usize);
			let samples_sent = adpcm_len.saturating_sub(BLOCK_HEADER_SIZE * blocks_in_payload)
				* SAMPLES_PER_BYTE
				+ blocks_in_payload;

			// Time to play the sample in seconds
			let play_length = samples_sent as f32 / sample_rate as f32;
			tokio::time::sleep(std::time::Duration::from_secs_f32(play_length)).await;
		}

		self.talk_stop().await?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::bc_protocol::connection::mock::{MockConnection, reply_200_empty, reply_err_code};
	use std::time::Duration;

	fn ok_talk_config() -> TalkConfig {
		TalkConfig {
			version: "1.1".to_string(),
			channel_id: 0,
			duplex: "FDX".to_string(),
			audio_stream_mode: "followVideoStream".to_string(),
			audio_config: AudioConfig {
				priority: None,
				audio_type: "adpcm".to_string(),
				sample_rate: 16000,
				sample_precision: 16,
				length_per_encoder: 1024,
				sound_track: "mono".to_string(),
			},
		}
	}

	fn bad_talk_config() -> TalkConfig {
		let mut t = ok_talk_config();
		t.audio_config.audio_type = "aac".to_string();
		t
	}

	// ── talk_stop ─────────────────────────────────────────────────────

	#[tokio::test]
	async fn talk_stop_happy_path_accepts_200() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		cam.talk_stop().await.expect("talk_stop 200");
	}

	#[tokio::test]
	async fn talk_stop_non_200_returns_unintelligible() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let err = cam.talk_stop().await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	// ── talk_ability ──────────────────────────────────────────────────

	#[tokio::test]
	async fn talk_ability_happy_path_parses_xml() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKABILITY)
			.reply_with(|req| {
				Bc::new_from_xml(
					BcMeta {
						msg_id: MSG_ID_TALKABILITY,
						channel_id: req.meta.channel_id,
						msg_num: req.meta.msg_num,
						stream_type: 0,
						response_code: 200,
						class: 0x6414,
					},
					BcXml {
						talk_ability: Some(TalkAbility {
							version: "1.1".to_string(),
							duplex_list: vec![DuplexList {
								duplex: "FDX".to_string(),
							}],
							audio_stream_mode_list: vec![AudioStreamModeList {
								audio_stream_mode: "followVideoStream".to_string(),
							}],
							audio_config_list: vec![AudioConfigList {
								audio_config: ok_talk_config().audio_config,
							}],
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let ability = cam.talk_ability().await.expect("ability");
		assert_eq!(ability.version, "1.1");
		assert_eq!(ability.duplex_list.len(), 1);
		assert_eq!(ability.audio_config_list.len(), 1);
	}

	#[tokio::test]
	async fn talk_ability_missing_xml_returns_unintelligible() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKABILITY)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let err = cam.talk_ability().await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	// ── talk: input validation ────────────────────────────────────────

	#[tokio::test]
	async fn talk_rejects_non_adpcm_audio_type_before_network() {
		// Non-adpcm config must short-circuit before any reply is
		// scripted; we script nothing to prove no traffic occurs.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_none()
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let err = cam
			.talk(&[0u8; 16], bad_talk_config())
			.await
			.expect_err("should fail");
		assert!(matches!(err, Error::UnknownTalkEncoding));
	}

	#[tokio::test]
	async fn talk_rejects_zero_sample_rate_before_network() {
		let mut cfg = ok_talk_config();
		cfg.audio_config.sample_rate = 0;
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.talk(&[0u8; 16], cfg).await.expect_err("should fail");
		assert!(matches!(err, Error::Other(_)));
	}

	// ── talk: non-200 on TalkConfig reply ─────────────────────────────

	#[tokio::test]
	async fn talk_non_200_on_talkconfig_returns_unintelligible() {
		// TalkConfig reply returns 500 directly (not 422, so no retry).
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let r = tokio::time::timeout(
			Duration::from_millis(300),
			cam.talk(&[0u8; 16], ok_talk_config()),
		)
		.await;
		let err = r.expect("timely").expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	// ── talk: happy path (TALKCONFIG 200 → TALK block → TALKRESET) ────

	#[tokio::test]
	async fn talk_happy_path_sends_config_block_and_stop() {
		// Use tiny adpcm buffer (16 bytes) so only one TALK payload
		// fires and std::thread::sleep stays well under 100 ms.
		let mut cfg = ok_talk_config();
		cfg.audio_config.length_per_encoder = 8; // block_size = 4
		cfg.audio_config.sample_rate = 16000;

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALK)
			.reply_none()
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let adpcm = vec![0u8; 32]; // 4 blocks of 8 bytes = 1 payload
		tokio::time::timeout(Duration::from_secs(2), cam.talk(&adpcm, cfg))
			.await
			.expect("no hang")
			.expect("talk ok");
	}

	#[tokio::test]
	async fn talk_retries_once_on_422_config_reply() {
		// First TALKCONFIG → 422, which triggers an internal talk_stop
		// (TALKRESET → 200) and a retry (TALKCONFIG → 200).
		let mut cfg = ok_talk_config();
		cfg.audio_config.length_per_encoder = 8;
		cfg.audio_config.sample_rate = 16000;

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(|req| reply_err_code(req, 422))
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALK)
			.reply_none()
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let adpcm = vec![0u8; 32];
		tokio::time::timeout(Duration::from_secs(2), cam.talk(&adpcm, cfg))
			.await
			.expect("no hang")
			.expect("retry ok");
	}

	/// `talk_ability` gates on `two_way_audio`: a camera whose ability
	/// report lacks it surfaces `MissingAbility` (CLI exit code 6)
	/// rather than the generic protocol error it would reply with.
	#[tokio::test]
	async fn talk_ability_gates_on_two_way_audio_ability() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_abilities_reported().await;
		assert!(matches!(
			cam.talk_ability().await.unwrap_err(),
			Error::MissingAbility { .. }
		));
	}

	/// `talk` and its talk-stop are not gated: battery doorbells accept
	/// talk without advertising `two_way_audio`, so the full TALKCONFIG
	/// → TALK → TALKRESET exchange runs on a camera that lacks it.
	#[tokio::test]
	async fn talk_runs_without_two_way_audio_ability() {
		let mut cfg = ok_talk_config();
		cfg.audio_config.length_per_encoder = 8;
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALK)
			.reply_none()
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_abilities_reported().await;
		tokio::time::timeout(Duration::from_secs(2), cam.talk(&[0u8; 32], cfg))
			.await
			.expect("no hang")
			.expect("talk ok without the ability");
	}

	/// A last payload with fewer than four blocks (here 40 bytes of
	/// 8-byte blocks: one full payload of 32, then one block of 8) used
	/// to underflow `adpcm_len - 16` and panic.
	#[tokio::test]
	async fn talk_short_last_payload_does_not_underflow() {
		let mut cfg = ok_talk_config();
		cfg.audio_config.length_per_encoder = 8; // 8-byte blocks
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALK)
			.reply_none()
			.expect_msg(MSG_ID_TALK)
			.reply_none()
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		tokio::time::timeout(Duration::from_secs(2), cam.talk(&[0u8; 40], cfg))
			.await
			.expect("no hang")
			.expect("talk ok");
	}
}
