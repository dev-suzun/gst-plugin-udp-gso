# gst-plugin-udp-gso

`gst-plugin-udp-gso` is an experimental Linux-only GStreamer plugin for
evaluating high-performance UDP transmission. Its element is named
`udpgsosink`.

This first implementation provides:

- one-datagram `sendmsg` mode;
- `sendmmsg` batching for `GstBufferList` input;
- Linux `UDP_SEGMENT` (UDP GSO) for consecutive equal-sized datagrams;
- automatic fallback from failed GSO operations to `sendmmsg`;
- scatter/gather mapping of separate `GstMemory` blocks;
- read-only counters for packets, bytes, calls, batches, segments, and fallback;
- pure-C tests for the grouping algorithm and the kernel's GSO behavior.

It deliberately does **not** segment a single `GstBuffer`: one buffer represents
one UDP datagram.  GSO is used only when upstream supplies a `GstBufferList`,
where each list member is already an independent datagram.

## Requirements

- Linux with UDP GSO support (Linux 4.18 or later for `UDP_SEGMENT`);
- GStreamer and GStreamer Base development packages, version 1.20 or later;
- GCC or Clang;
- Meson and Ninja.

On Ubuntu 24.04 the usual development packages are:

```bash
sudo apt install build-essential meson ninja-build pkg-config \
  libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
  gstreamer1.0-tools gstreamer1.0-plugins-good
```

## Build

```bash
meson setup build
meson compile -C build
meson test -C build --print-errorlogs
```

Inspect the uninstalled element:

```bash
GST_PLUGIN_PATH="$PWD/build/src" gst-inspect-1.0 udpgsosink
```

## Basic smoke test

Start a receiver:

```bash
gst-launch-1.0 -v udpsrc port=5004 \
  caps="application/x-rtp,media=video,encoding-name=H264,clock-rate=90000,payload=96" \
  ! rtpjitterbuffer latency=50 \
  ! rtph264depay ! h264parse ! avdec_h264 ! autovideosink
```

Start a sender, replacing `x264enc` with a hardware encoder when available:

```bash
GST_PLUGIN_PATH="$PWD/build/src" gst-launch-1.0 -v \
  videotestsrc is-live=true \
  ! video/x-raw,width=3840,height=2160,framerate=30/1 \
  ! x264enc tune=zerolatency speed-preset=ultrafast bitrate=25000 \
  ! rtph264pay pt=96 mtu=1200 aggregate-mode=zero-latency \
  ! udpgsosink host=RECEIVER_IP port=5004 io-mode=auto sync=false
```

Whether the RTP payloader passes buffers individually or as a `GstBufferList`
depends on the pipeline and payloading path.  Individual buffers exercise the
correct baseline path but cannot be combined by GSO without adding an internal
queue and a bounded batching delay.  A dedicated integration test that pushes
`GstBufferList` is planned for the next milestone.

## Properties

| Property | Default | Meaning |
|---|---:|---|
| `host` | `127.0.0.1` | Destination hostname or IP address |
| `port` | `5004` | Destination UDP port |
| `io-mode` | `auto` | `auto`, `sendmsg`, `sendmmsg`, or `gso` |
| `batch-size` | `32` | Maximum messages per `sendmmsg` call |
| `gso-min-segments` | `4` | Smallest useful equal-size GSO run |
| `gso-max-segments` | `32` | Maximum segments in one GSO superpacket |
| `fallback` | `true` | Retry feature-related GSO errors with `sendmmsg` |
| `send-buffer-size` | `0` | Requested `SO_SNDBUF`; zero keeps the default |

Read-only counters:

- `gso-supported`
- `packets-sent`
- `bytes-sent`
- `system-calls`
- `sendmmsg-batches`
- `gso-batches`
- `gso-segments`
- `fallback-count`

## Mode behavior

`sendmsg`
: Sends one datagram per system call.  This is the intentionally simple
  baseline.

`sendmmsg`
: Sends list members in bounded batches.  The implementation handles partial
  completion and interrupted calls.

`gso`
: Groups consecutive equal-sized list members, subject to the configured GSO
  and `IOV_MAX` limits.  Incompatible tails use `sendmmsg`.

`auto`
: Uses the GSO grouping policy when `UDP_SEGMENT` is available and otherwise
  uses `sendmmsg`.

## Current limitations

- one destination per element;
- connected unicast UDP socket;
- no multicast-specific configuration;
- no custom socket injection;
- no internal packet accumulation;
- no `SO_TXTIME` pacing yet;
- no RTP parsing yet;
- blocking socket behavior only.

These limits are intentional for the first benchmarkable milestone.  The next
milestone should add a bounded transmission queue and `SO_TXTIME`/`SCM_TXTIME`
behind an independent `pacing-mode` property, without changing the GSO baseline.

## Benchmark warning

Compare against standard `udpsink` using both individual buffers and actual
`GstBufferList` input.  Current GStreamer `multiudpsink` already performs
scatter/gather and multi-message sending, so the thesis must not attribute all
batching gains to this plugin.  The specific GSO hypothesis is that carrying
multiple datagrams as one object farther through the Linux transmit path reduces
CPU cycles per packet beyond ordinary multi-message submission.

