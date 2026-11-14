use anyhow::{Context, Result};
use rtc::data_channel::{RTCDataChannelId, RTCDataChannelInit};
use rtc::interceptor::{
    NackGeneratorBuilder, NackGeneratorInterceptor, NoopInterceptor, ReceiverReportBuilder,
    ReceiverReportInterceptor, Registry,
};
use rtc::peer_connection::RTCPeerConnectionBuilder;
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::media_engine::MediaEngine;
use rtc::peer_connection::configuration::setting_engine::SettingEngine;
use rtc::peer_connection::transport::RTCIceServer;
use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;
use std::time::Duration;

/// How often the receive log is checked for missing RTP sequence numbers and a NACK is sent. At
/// 720p60 a frame is ~17 ms, so a loss is reported before the next frame has fully arrived.
const NACK_INTERVAL: Duration = Duration::from_millis(15);
/// Receive-log size in packets (a power of two); ~2 s of 720p video at 2 Mbps.
const NACK_RECEIVE_LOG_SIZE: u16 = 512;
/// A missing packet is NACKed at most this many times (~45 ms), then given up on. Together with
/// `rtp::LONG_FRAME_HOLD` this bounds how long a damaged frame waits for a retransmission.
const NACK_MAX_PER_PACKET: u16 = 3;

/// Interceptor chain of every peer: NACK generation (RFC 4585) on top of RTCP receiver reports
/// (RFC 3550). rtc-rs is sans-I/O and ships neither by default, so without this chain the sender
/// never hears about packet loss and nothing is ever retransmitted.
pub(crate) type Interceptors =
    NackGeneratorInterceptor<ReceiverReportInterceptor<NoopInterceptor>>;
/// The peer connection type used throughout the crate (rtc-rs makes the interceptor chain part
/// of the type).
pub(crate) type RTCPeerConnection = rtc::peer_connection::RTCPeerConnection<Interceptors>;

pub(crate) struct RtcDataChannelConfig {
    pub label: &'static str,
    pub protocol: &'static str,
    pub ordered: bool,
    pub max_packet_life_time: Option<u16>,
    pub max_retransmits: Option<u16>,
}

/// Builds the provider-neutral WebRTC peer. Providers supply only their negotiated codecs,
/// ICE servers and data-channel descriptions.
pub(crate) fn create(
    media_engine: MediaEngine,
    ice_servers: Vec<RTCIceServer>,
    channels: &[RtcDataChannelConfig],
) -> Result<(RTCPeerConnection, Vec<RTCDataChannelId>)> {
    let configuration = RTCConfigurationBuilder::new()
        .with_ice_servers(ice_servers)
        .build();
    let mut setting_engine = SettingEngine::default();
    setting_engine.set_ice_connection_attempts(Some(Duration::from_millis(200)), Some(75));

    // The NACK generator binds only to remote streams whose negotiated codec carries the "nack"
    // feedback (the intersection of our offer and the remote answer), so it stays inert against
    // a sender that does not offer retransmission.
    let interceptors = Registry::new()
        .with(ReceiverReportBuilder::new().build())
        .with(
            NackGeneratorBuilder::new()
                .with_size(NACK_RECEIVE_LOG_SIZE)
                .with_interval(NACK_INTERVAL)
                .with_max_nacks_per_packet(NACK_MAX_PER_PACKET)
                .build(),
        );

    let mut peer = RTCPeerConnectionBuilder::new()
        .with_configuration(configuration)
        .with_setting_engine(setting_engine)
        .with_media_engine(media_engine)
        .with_interceptor_registry(interceptors)
        .build()
        .context("failed to create rtc peer connection")?;

    let mut channel_ids = Vec::with_capacity(channels.len());
    for channel in channels {
        let init = RTCDataChannelInit {
            ordered: channel.ordered,
            max_packet_life_time: channel.max_packet_life_time,
            max_retransmits: channel.max_retransmits,
            protocol: channel.protocol.to_owned(),
            ..Default::default()
        };
        let data_channel = peer
            .create_data_channel(channel.label, Some(init))
            .with_context(|| format!("failed to create {} data channel", channel.label))?;
        channel_ids.push(data_channel.id());
    }

    peer.add_transceiver_from_kind(RtpCodecKind::Video, None)
        .context("failed to add video transceiver")?;
    peer.add_transceiver_from_kind(RtpCodecKind::Audio, None)
        .context("failed to add audio transceiver")?;

    Ok((peer, channel_ids))
}
