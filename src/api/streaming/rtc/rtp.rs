use crate::streaming::video::{STREAM_HEIGHT, STREAM_WIDTH, VideoDecodeWorker};
use bytes::Bytes;
use h264_reader::annexb::AnnexBReader;
use h264_reader::nal::sps::SeqParameterSet;
use h264_reader::nal::{Nal, RefNal, UnitType};
use h264_reader::push::NalInterest;
use rtc::rtp::Packet;
use rtc::rtp::codec::h264::H264Packet;
use rtc::rtp::codec::opus::OpusPacket;
use rtc::rtp::packetizer::Depacketizer;
use rtc_media::io::sample_builder::SampleBuilder;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

const MAX_PENDING_AUDIO_PACKETS: usize = 32;
const MAX_H264_ACCESS_UNIT_BYTES: usize = 2 * 1024 * 1024;
const VIDEO_RTP_CLOCK_RATE: u32 = 90_000;
const AUDIO_MAX_LATE_PACKETS: u16 = 32;
const LOW_FPS_DAMAGE_LIMIT: u8 = 8;
const HIGH_FPS_DAMAGE_LIMIT: u8 = 3;
/// An incomplete video frame is kept for a while once packets of a newer frame arrive, measured
/// from its first packet, so a NACK retransmission or a reordered packet can still complete it.
/// Before this hold the frame was dropped (and a keyframe requested) the moment a newer frame
/// started. The hold is adaptive: while no retransmission has been seen it is about one frame
/// interval, so a loss the sender never repairs freezes for roughly as long as before and the
/// keyframe request is delayed by at most that; once retransmissions do arrive (`recovered`
/// within `RECOVERY_WINDOW`) it grows to cover NACK interval + RTT + jitter.
const SHORT_FRAME_HOLD: Duration = Duration::from_millis(20);
const LONG_FRAME_HOLD: Duration = Duration::from_millis(80);
/// A recovery keeps the long hold on for this long; without another one it falls back.
const RECOVERY_WINDOW: Duration = Duration::from_secs(2);
/// On the short hold, every N-th drop makes the next damaged frame probe with the long hold, so
/// retransmissions slower than the short hold still get a chance to show up and switch the mode.
const LONG_HOLD_PROBE_EVERY: u32 = 8;
/// Most distinct newer frames parked during one hold. Their replay is a burst of decoder
/// submissions; the decode queue holds 2-6 access units (video/worker.rs), so with the frame
/// that completes this stays below the limit while the queue is near empty, as it normally is.
const MAX_DEFERRED_FRAMES: usize = 3;
/// Upper bound on parked packets (~0.4 MB at MTU-sized RTP payloads; 80 ms of a 2 Mbps stream
/// is ~20 packets, a parked keyframe up to ~80).
const MAX_DEFERRED_PACKETS: usize = 256;

#[derive(Default)]
pub(super) struct VideoSampleStats {
    pub dropped: u32,
    /// Frames completed after a newer frame had already started: finished by a retransmitted or
    /// reordered packet, which before the hold would have been counted in `dropped`.
    pub recovered: u32,
    pub source_frame_duration_us: Option<u64>,
    pub encoded_resolution: Option<(u32, u32)>,
}

/// What `VideoRtp::receive_one` asks of the packet loop after handling one packet.
enum Step {
    Done,
    /// The pending slot was emptied for the frame with this RTP timestamp; parked packets of
    /// newer frames must now be replayed, oldest frame first.
    Replay { base: u32 },
}

pub(super) struct AudioRtp {
    samples: SampleBuilder<OpusPacket>,
    payload_type: u8,
}

impl AudioRtp {
    pub(super) fn new(sample_rate: u32, payload_type: u8) -> Self {
        Self {
            samples: SampleBuilder::new(AUDIO_MAX_LATE_PACKETS, OpusPacket, sample_rate)
                .with_max_time_delay(std::time::Duration::from_millis(80)),
            payload_type,
        }
    }

    pub(super) fn receive(&mut self, packet: Packet, audio_packets: &mut Vec<Bytes>) {
        if packet.header.payload_type != self.payload_type {
            return;
        }

        self.samples.push(packet);
        while audio_packets.len() < MAX_PENDING_AUDIO_PACKETS {
            let Some(sample) = self.samples.pop() else {
                break;
            };
            audio_packets.push(sample.data);
        }
    }
}

pub(super) struct VideoRtp {
    depacketizer: H264Packet,
    pending: Option<PendingVideoFrame>,
    next_sequence: Option<u16>,
    last_frame_timestamp: Option<u32>,
    source_frame_duration_us: Option<u64>,
    damage_score: u8,
    stream_too_large: bool,
    waiting_for_keyframe: bool,
    /// Packets of frames newer than `pending`, parked with their arrival time while the
    /// incomplete `pending` frame is held (see `SHORT_FRAME_HOLD`/`LONG_FRAME_HOLD`).
    deferred: Vec<(Instant, Packet)>,
    /// Distinct RTP timestamps in `deferred`.
    deferred_frames: usize,
    /// The next completed frame follows a drop and must not erase its damage point.
    frame_after_damage: bool,
    /// When a held frame was last completed late (a retransmission or reordered packet arrived).
    last_recovery_at: Option<Instant>,
    /// Drops on the short hold since the last long-hold probe.
    drops_since_probe: u32,
    /// The next held frame uses the long hold as a probe.
    probe_long_hold: bool,
    /// Timestamp of the last dropped frame: its late packets must not reopen a frame that can
    /// never complete. Cleared when a newer frame completes.
    discard_before: Option<u32>,
}

struct PendingVideoFrame {
    timestamp: u32,
    first_packet_at: Instant,
    packets: Vec<Packet>,
}

enum FrameAssembly {
    Pending,
    Complete { data: Bytes, marker_sequence: u16 },
    Invalid,
}

impl PendingVideoFrame {
    fn new(packet: Packet, arrived_at: Instant) -> Self {
        Self {
            timestamp: packet.header.timestamp,
            first_packet_at: arrived_at,
            packets: vec![packet],
        }
    }

    fn insert(&mut self, packet: Packet) {
        if !self
            .packets
            .iter()
            .any(|existing| existing.header.sequence_number == packet.header.sequence_number)
        {
            self.packets.push(packet);
        }
    }

    fn marker_sequence(&self) -> Option<u16> {
        self.packets
            .iter()
            .find(|packet| packet.header.marker)
            .map(|packet| packet.header.sequence_number)
    }

    fn assemble(
        &self,
        depacketizer: &mut H264Packet,
        expected_sequence: Option<u16>,
    ) -> FrameAssembly {
        let Some(marker_sequence) = self.marker_sequence() else {
            return FrameAssembly::Pending;
        };
        let mut packets = self.packets.iter().collect::<Vec<_>>();
        packets.sort_unstable_by_key(|packet| {
            std::cmp::Reverse(marker_sequence.wrapping_sub(packet.header.sequence_number))
        });
        let Some(first) = packets.first() else {
            return FrameAssembly::Pending;
        };
        if expected_sequence.is_some_and(|expected| first.header.sequence_number != expected)
            || !depacketizer.is_partition_head(&first.payload)
        {
            return FrameAssembly::Pending;
        }
        if packets.windows(2).any(|pair| {
            pair[1].header.sequence_number != pair[0].header.sequence_number.wrapping_add(1)
        }) {
            return FrameAssembly::Pending;
        }

        *depacketizer = H264Packet::default();
        let mut data = Vec::new();
        for packet in packets {
            let Ok(nalu) = depacketizer.depacketize(&packet.payload) else {
                *depacketizer = H264Packet::default();
                return FrameAssembly::Invalid;
            };
            data.extend_from_slice(&nalu);
            if data.len() > MAX_H264_ACCESS_UNIT_BYTES {
                *depacketizer = H264Packet::default();
                return FrameAssembly::Invalid;
            }
        }
        *depacketizer = H264Packet::default();
        FrameAssembly::Complete {
            data: Bytes::from(data),
            marker_sequence,
        }
    }
}

impl VideoRtp {
    pub(super) fn new() -> Self {
        Self {
            depacketizer: H264Packet::default(),
            pending: None,
            next_sequence: None,
            last_frame_timestamp: None,
            source_frame_duration_us: None,
            damage_score: 0,
            stream_too_large: false,
            waiting_for_keyframe: false,
            deferred: Vec::new(),
            deferred_frames: 0,
            frame_after_damage: false,
            last_recovery_at: None,
            drops_since_probe: 0,
            probe_long_hold: false,
            discard_before: None,
        }
    }

    pub(super) fn waiting_for_keyframe(&self) -> bool {
        self.waiting_for_keyframe
    }

    pub(super) fn wait_for_keyframe(&mut self) {
        self.waiting_for_keyframe = true;
    }

    pub(super) fn receive(
        &mut self,
        worker: &VideoDecodeWorker,
        packet: Packet,
        keyframe_requested: &mut bool,
    ) -> VideoSampleStats {
        let mut stats = VideoSampleStats::default();
        let mut queue = VecDeque::new();
        queue.push_back((Instant::now(), packet));
        while let Some((arrived_at, packet)) = queue.pop_front() {
            if let Step::Replay { base } =
                self.receive_one(worker, arrived_at, packet, keyframe_requested, &mut stats)
            {
                // The pending slot is free again: parked packets of newer frames go first, oldest
                // frame first (every parked frame is newer than `base`, so the distance sorts).
                let mut deferred = std::mem::take(&mut self.deferred);
                self.deferred_frames = 0;
                deferred.sort_by_key(|(_, parked)| parked.header.timestamp.wrapping_sub(base));
                for parked in deferred.into_iter().rev() {
                    queue.push_front(parked);
                }
            }
        }
        stats
    }

    /// Whether a packet with this timestamp may be parked without exceeding the frame and packet
    /// caps (a frame already parked may always grow).
    fn can_defer(&self, timestamp: u32) -> bool {
        self.deferred.len() < MAX_DEFERRED_PACKETS
            && (self.deferred_frames < MAX_DEFERRED_FRAMES
                || self
                    .deferred
                    .iter()
                    .any(|(_, parked)| parked.header.timestamp == timestamp))
    }

    /// Parks a packet of a newer frame while an incomplete older frame is held. Deduplicated by
    /// sequence number, so a retransmission that arrives twice costs nothing.
    fn defer(&mut self, arrived_at: Instant, packet: Packet) {
        if self
            .deferred
            .iter()
            .any(|(_, parked)| parked.header.sequence_number == packet.header.sequence_number)
        {
            return;
        }
        if !self
            .deferred
            .iter()
            .any(|(_, parked)| parked.header.timestamp == packet.header.timestamp)
        {
            self.deferred_frames += 1;
        }
        self.deferred.push((arrived_at, packet));
    }

    /// The hold for the pending frame: long once retransmissions have been seen recently or as a
    /// probe, short otherwise.
    fn frame_hold(&self, now: Instant) -> Duration {
        let recovering = self
            .last_recovery_at
            .is_some_and(|at| now.duration_since(at) < RECOVERY_WINDOW);
        if recovering || self.probe_long_hold {
            LONG_FRAME_HOLD
        } else {
            SHORT_FRAME_HOLD
        }
    }

    /// Bookkeeping for a dropped frame: its late packets are ignored from now on, and on the
    /// short hold every `LONG_HOLD_PROBE_EVERY`-th drop arms a long-hold probe.
    fn note_drop(&mut self, timestamp: u32, now: Instant) {
        self.discard_before = Some(timestamp);
        self.frame_after_damage = true;
        let recovering = self
            .last_recovery_at
            .is_some_and(|at| now.duration_since(at) < RECOVERY_WINDOW);
        if self.probe_long_hold || recovering {
            self.probe_long_hold = false;
            self.drops_since_probe = 0;
        } else {
            self.drops_since_probe += 1;
            if self.drops_since_probe >= LONG_HOLD_PROBE_EVERY {
                self.drops_since_probe = 0;
                self.probe_long_hold = true;
            }
        }
    }

    /// What follows once the pending slot has been emptied for the frame with timestamp `base`.
    fn after_frame(&self, base: u32) -> Step {
        if self.deferred.is_empty() {
            Step::Done
        } else {
            Step::Replay { base }
        }
    }

    fn receive_one(
        &mut self,
        worker: &VideoDecodeWorker,
        arrived_at: Instant,
        packet: Packet,
        keyframe_requested: &mut bool,
        stats: &mut VideoSampleStats,
    ) -> Step {
        if packet.payload.is_empty() {
            if self.next_sequence == Some(packet.header.sequence_number) {
                self.next_sequence = Some(packet.header.sequence_number.wrapping_add(1));
            }
            return Step::Done;
        }

        let packet_timestamp = packet.header.timestamp;
        if let Some(pending) = &self.pending
            && pending.timestamp != packet_timestamp
        {
            if !timestamp_is_newer(packet_timestamp, pending.timestamp) {
                return Step::Done;
            }
            // A newer frame has started while this one is incomplete. Hold the incomplete frame
            // for a short while so a NACK retransmission or a reordered packet can still finish
            // it; the newer frame's packets wait in `deferred` meanwhile.
            if arrived_at.duration_since(pending.first_packet_at) < self.frame_hold(arrived_at)
                && self.can_defer(packet_timestamp)
            {
                self.defer(arrived_at, packet);
                return Step::Done;
            }
            if let Some(incomplete) = self.pending.take() {
                self.next_sequence = incomplete
                    .marker_sequence()
                    .map(|sequence| sequence.wrapping_add(1));
                self.depacketizer = H264Packet::default();
                *keyframe_requested = true;
                self.record_damage(worker);
                self.note_drop(incomplete.timestamp, arrived_at);
                stats.dropped = stats.dropped.saturating_add(1);
                // The packet that ended the hold is replayed together with the parked ones, in
                // frame order.
                self.defer(arrived_at, packet);
                return Step::Replay {
                    base: incomplete.timestamp,
                };
            }
        }
        if self.pending.is_none() {
            if self
                .last_frame_timestamp
                .is_some_and(|last| !timestamp_is_newer(packet_timestamp, last))
                || self
                    .discard_before
                    .is_some_and(|dropped| !timestamp_is_newer(packet_timestamp, dropped))
            {
                return Step::Done;
            }
            self.pending = Some(PendingVideoFrame::new(packet, arrived_at));
        } else if let Some(pending) = &mut self.pending {
            pending.insert(packet);
        }

        let assembly = self
            .pending
            .as_ref()
            .map(|pending| pending.assemble(&mut self.depacketizer, self.next_sequence));
        let Some(assembly) = assembly else {
            return Step::Done;
        };
        let (data, marker_sequence) = match assembly {
            FrameAssembly::Pending => return Step::Done,
            FrameAssembly::Invalid => {
                self.pending = None;
                self.next_sequence = None;
                *keyframe_requested = true;
                self.record_damage(worker);
                self.note_drop(packet_timestamp, arrived_at);
                stats.dropped = stats.dropped.saturating_add(1);
                return self.after_frame(packet_timestamp);
            }
            FrameAssembly::Complete {
                data,
                marker_sequence,
            } => (data, marker_sequence),
        };
        let completed = self.pending.take().expect("assembled pending video frame");
        let base = completed.timestamp;
        // A frame that completes after packets of a newer frame arrived was finished by a
        // retransmitted or reordered packet: before the hold it would have been dropped. It also
        // proves retransmissions reach us, which switches the hold to the long one for a while.
        if !self.deferred.is_empty() {
            stats.recovered = stats.recovered.saturating_add(1);
            self.last_recovery_at = Some(arrived_at);
            self.drops_since_probe = 0;
            self.probe_long_hold = false;
        }
        // Record both average and worst-case RTP assembly time for the stream HUD.
        let assembly_us = completed.first_packet_at.elapsed().as_micros() as u64;
        crate::streaming::video::metrics::METRICS
            .rtp_assembly_sum_us
            .fetch_add(assembly_us, Ordering::Relaxed);
        crate::streaming::video::metrics::METRICS
            .rtp_assembly_count
            .fetch_add(1, Ordering::Relaxed);
        crate::streaming::video::metrics::METRICS
            .rtp_assembly_max_us
            .fetch_max(assembly_us, Ordering::Relaxed);
        self.next_sequence = Some(marker_sequence.wrapping_add(1));
        stats.source_frame_duration_us = self.last_frame_timestamp.map(|previous| {
            u64::from(completed.timestamp.wrapping_sub(previous)) * 1_000_000
                / u64::from(VIDEO_RTP_CLOCK_RATE)
        });
        if let Some(duration) = stats.source_frame_duration_us {
            self.source_frame_duration_us = Some(
                self.source_frame_duration_us
                    .map(|average| (average * 7 + duration) / 8)
                    .unwrap_or(duration),
            );
        }
        self.last_frame_timestamp = Some(completed.timestamp);
        // A newer frame completed: the last drop's late packets are now rejected by
        // `last_frame_timestamp` alone, so the marker can go (it must not outlive a timestamp
        // wrap or a sender timestamp jump, which would otherwise freeze the video).
        self.discard_before = None;

        let unit = inspect_h264_access_unit(&data);
        stats.encoded_resolution = unit.resolution;
        let sample_too_large = unit
            .resolution
            .is_some_and(|(width, height)| width > STREAM_WIDTH || height > STREAM_HEIGHT);
        if sample_too_large {
            eprintln!(
                "Dropping H264 access unit larger than decoder: {:?} > {}x{}",
                unit.resolution, STREAM_WIDTH, STREAM_HEIGHT
            );
            self.stream_too_large = true;
            // Flush queued decoder work once, then wait for a compatible IDR instead of feeding
            // frames that the Vita hardware cannot decode.
            *keyframe_requested = true;
            if !self.waiting_for_keyframe {
                worker.begin_resync();
            }
            self.waiting_for_keyframe = true;
            stats.dropped = stats.dropped.saturating_add(1);
            return self.after_frame(base);
        }
        if self.stream_too_large {
            if unit.resolution.is_none() || !unit.has_idr {
                *keyframe_requested = true;
                self.waiting_for_keyframe = true;
                stats.dropped = stats.dropped.saturating_add(1);
                return self.after_frame(base);
            }
            self.stream_too_large = false;
        }
        if self.waiting_for_keyframe {
            // Later keyframes may contain only IDR; AVCDEC retains SPS/PPS across resyncs.
            if !unit.has_idr {
                *keyframe_requested = true;
                stats.dropped = stats.dropped.saturating_add(1);
                return self.after_frame(base);
            }
            self.waiting_for_keyframe = false;
            self.damage_score = 0;
        } else if !self.frame_after_damage {
            self.damage_score = self.damage_score.saturating_sub(1);
        }
        self.frame_after_damage = false;

        if !worker.submit_access_unit(data.to_vec(), self.source_frame_duration_us) {
            eprintln!("Video decoder queue is full; continuing while requesting a keyframe");
            *keyframe_requested = true;
            stats.dropped = stats.dropped.saturating_add(1);
        }
        self.after_frame(base)
    }

    fn record_damage(&mut self, worker: &VideoDecodeWorker) {
        if self.waiting_for_keyframe {
            return;
        }

        self.damage_score = self.damage_score.saturating_add(1);
        let source_fps = self
            .source_frame_duration_us
            .filter(|duration| *duration > 0)
            .map(|duration| 1_000_000 / duration)
            .unwrap_or(30);
        let damage_limit = if source_fps <= 30 {
            LOW_FPS_DAMAGE_LIMIT
        } else if source_fps >= 60 {
            HIGH_FPS_DAMAGE_LIMIT
        } else {
            LOW_FPS_DAMAGE_LIMIT
                - (((source_fps - 30) * u64::from(LOW_FPS_DAMAGE_LIMIT - HIGH_FPS_DAMAGE_LIMIT)
                    + 29)
                    / 30) as u8
        };
        if self.damage_score < damage_limit {
            return;
        }

        worker.begin_resync();
        self.waiting_for_keyframe = true;
        self.damage_score = 0;
    }
}

fn timestamp_is_newer(candidate: u32, reference: u32) -> bool {
    let distance = candidate.wrapping_sub(reference);
    distance != 0 && distance < (1 << 31)
}

struct AccessUnitInfo {
    has_idr: bool,
    resolution: Option<(u32, u32)>,
}

fn inspect_h264_access_unit(data: &[u8]) -> AccessUnitInfo {
    let mut info = AccessUnitInfo {
        has_idr: false,
        resolution: None,
    };
    let mut reader = AnnexBReader::accumulate(|nal: RefNal<'_>| {
        let Ok(header) = nal.header() else {
            return NalInterest::Ignore;
        };
        match header.nal_unit_type() {
            UnitType::SliceLayerWithoutPartitioningIdr => {
                info.has_idr = true;
                NalInterest::Ignore
            }
            UnitType::SeqParameterSet => {
                if nal.is_complete() {
                    info.resolution = SeqParameterSet::from_bits(nal.rbsp_bits())
                        .and_then(|sps| sps.pixel_dimensions())
                        .ok();
                }
                NalInterest::Buffer
            }
            _ => NalInterest::Ignore,
        }
    });
    reader.push(data);
    reader.reset();
    info
}
