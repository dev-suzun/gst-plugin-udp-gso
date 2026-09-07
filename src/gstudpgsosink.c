/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "gstudpgsosink.h"

#include "udpgsobatch.h"
#include "udpgsoschedule.h"

#include <errno.h>
#include <linux/net_tstamp.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#ifndef UDP_SEGMENT
#define UDP_SEGMENT 103
#endif

#ifndef SOL_UDP
#define SOL_UDP IPPROTO_UDP
#endif

#ifndef SCM_TXTIME
#define SCM_TXTIME SO_TXTIME
#endif

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 5004
#define DEFAULT_BATCH_SIZE 32
#define DEFAULT_GSO_MIN_SEGMENTS 4
#define DEFAULT_GSO_MAX_SEGMENTS 32
#define MAX_GSO_SEGMENTS 64
#define DEFAULT_MAX_QUEUE_PACKETS 256
#define DEFAULT_MAX_BATCH_DELAY_US 1000
#define DEFAULT_PACING_RATE UINT64_C (50000000)
#define DEFAULT_PACING_LEAD_TIME_US 2000
#define DEFAULT_MAX_PACING_HORIZON_US 100000
#define DEFAULT_WIRE_OVERHEAD_BYTES 42

GST_DEBUG_CATEGORY_STATIC (gst_udp_gso_sink_debug);
#define GST_CAT_DEFAULT gst_udp_gso_sink_debug

typedef enum
{
  GST_UDP_GSO_SINK_IO_AUTO,
  GST_UDP_GSO_SINK_IO_SENDMSG,
  GST_UDP_GSO_SINK_IO_SENDMMSG,
  GST_UDP_GSO_SINK_IO_GSO,
} GstUdpGsoSinkIoMode;

typedef enum
{
  GST_UDP_GSO_SINK_AGGREGATION_NONE,
  GST_UDP_GSO_SINK_AGGREGATION_BOUNDED,
} GstUdpGsoSinkAggregationMode;

typedef enum
{
  GST_UDP_GSO_SINK_BACKPRESSURE_BLOCK,
  GST_UDP_GSO_SINK_BACKPRESSURE_DROP_NEWEST,
  GST_UDP_GSO_SINK_BACKPRESSURE_ERROR,
} GstUdpGsoSinkBackpressure;

typedef enum
{
  GST_UDP_GSO_SINK_PACING_NONE,
  GST_UDP_GSO_SINK_PACING_AUTO,
  GST_UDP_GSO_SINK_PACING_TXTIME,
  GST_UDP_GSO_SINK_PACING_USERSPACE,
} GstUdpGsoSinkPacingMode;

typedef enum
{
  GST_UDP_GSO_SINK_LATE_SEND,
  GST_UDP_GSO_SINK_LATE_DROP,
} GstUdpGsoSinkLatePolicy;

typedef struct
{
  GstMapInfo *maps;
  struct iovec *iov;
  guint n_iov;
  gsize size;
} GstUdpGsoSinkMappedPacket;

typedef struct
{
  GstBuffer *buffer;
  gint64 enqueue_time_us;
  UdpGsoRtpInfo rtp;
} GstUdpGsoSinkQueuedPacket;

typedef struct
{
  guint64 packets_sent;
  guint64 bytes_sent;
  guint64 system_calls;
  guint64 sendmmsg_batches;
  guint64 gso_batches;
  guint64 gso_segments;
  guint64 fallback_count;
  guint64 dropped_packets;
  guint64 late_packets;
  guint64 queue_high_watermark;
  guint64 pacing_fallbacks;
  guint64 rtp_packets;
  guint64 rtp_invalid_packets;
  guint64 rtp_sequence_gaps;
  guint64 rtp_frame_boundaries;
} GstUdpGsoSinkStats;

struct _GstUdpGsoSink
{
  GstBaseSink parent;

  gchar *host;
  guint port;
  GstUdpGsoSinkIoMode io_mode;
  guint batch_size;
  guint gso_min_segments;
  guint gso_max_segments;
  gboolean fallback;
  gint send_buffer_size;

  GstUdpGsoSinkAggregationMode aggregation_mode;
  guint max_queue_packets;
  guint max_batch_delay_us;
  GstUdpGsoSinkBackpressure backpressure;

  GstUdpGsoSinkPacingMode pacing_mode;
  guint64 pacing_rate;
  guint pacing_lead_time_us;
  guint max_pacing_horizon_us;
  guint wire_overhead_bytes;
  GstUdpGsoSinkLatePolicy late_packet_policy;
  gboolean rtp_aware;

  gint fd;
  gint gso_supported;
  gint txtime_supported;
  gboolean txtime_active;
  gboolean gso_warning_emitted;
  gboolean txtime_warning_emitted;
  long iov_max;

  GMutex queue_lock;
  GCond queue_cond;
  GCond drain_cond;
  GQueue packet_queue;
  GThread *worker;
  gboolean worker_stop;
  gboolean worker_flush;
  gboolean worker_busy;
  gboolean unlocked;
  GstFlowReturn worker_flow;
  UdpGsoSchedule schedule;
  gboolean last_rtp_valid;
  guint16 last_rtp_sequence;
  guint32 last_rtp_timestamp;
  gboolean last_rtp_marker;

  GMutex stats_lock;
  GstUdpGsoSinkStats stats;
};

G_DEFINE_TYPE (GstUdpGsoSink, gst_udp_gso_sink, GST_TYPE_BASE_SINK)

static GstStaticPadTemplate sink_template = GST_STATIC_PAD_TEMPLATE (
    "sink", GST_PAD_SINK, GST_PAD_ALWAYS, GST_STATIC_CAPS_ANY);

enum
{
  PROP_0,
  PROP_HOST,
  PROP_PORT,
  PROP_IO_MODE,
  PROP_BATCH_SIZE,
  PROP_GSO_MIN_SEGMENTS,
  PROP_GSO_MAX_SEGMENTS,
  PROP_FALLBACK,
  PROP_SEND_BUFFER_SIZE,
  PROP_AGGREGATION_MODE,
  PROP_MAX_QUEUE_PACKETS,
  PROP_MAX_BATCH_DELAY_US,
  PROP_BACKPRESSURE,
  PROP_PACING_MODE,
  PROP_PACING_RATE,
  PROP_PACING_LEAD_TIME_US,
  PROP_MAX_PACING_HORIZON_US,
  PROP_WIRE_OVERHEAD_BYTES,
  PROP_LATE_PACKET_POLICY,
  PROP_RTP_AWARE,
  PROP_GSO_SUPPORTED,
  PROP_TXTIME_SUPPORTED,
  PROP_QUEUED_PACKETS,
  PROP_PACKETS_SENT,
  PROP_BYTES_SENT,
  PROP_SYSTEM_CALLS,
  PROP_SENDMMSG_BATCHES,
  PROP_GSO_BATCHES,
  PROP_GSO_SEGMENTS,
  PROP_FALLBACK_COUNT,
  PROP_DROPPED_PACKETS,
  PROP_LATE_PACKETS,
  PROP_QUEUE_HIGH_WATERMARK,
  PROP_PACING_FALLBACKS,
  PROP_RTP_PACKETS,
  PROP_RTP_INVALID_PACKETS,
  PROP_RTP_SEQUENCE_GAPS,
  PROP_RTP_FRAME_BOUNDARIES,
};

static GType
gst_udp_gso_sink_io_mode_get_type (void)
{
  static gsize type = 0;

  if (g_once_init_enter (&type)) {
    static const GEnumValue values[] = {
      { GST_UDP_GSO_SINK_IO_AUTO, "Choose GSO or batched sends automatically", "auto" },
      { GST_UDP_GSO_SINK_IO_SENDMSG, "One sendmsg call per datagram", "sendmsg" },
      { GST_UDP_GSO_SINK_IO_SENDMMSG, "Batch datagrams with sendmmsg", "sendmmsg" },
      { GST_UDP_GSO_SINK_IO_GSO, "Use UDP segmentation offload when possible", "gso" },
      { 0, NULL, NULL }
    };
    GType registered = g_enum_register_static ("GstUdpGsoSinkIoMode", values);
    g_once_init_leave (&type, registered);
  }

  return (GType) type;
}

#define GST_TYPE_UDP_GSO_SINK_IO_MODE (gst_udp_gso_sink_io_mode_get_type ())

static GType
gst_udp_gso_sink_aggregation_mode_get_type (void)
{
  static gsize type = 0;

  if (g_once_init_enter (&type)) {
    static const GEnumValue values[] = {
      { GST_UDP_GSO_SINK_AGGREGATION_NONE,
        "Send each input call synchronously", "none" },
      { GST_UDP_GSO_SINK_AGGREGATION_BOUNDED,
        "Accumulate datagrams in a bounded worker queue", "bounded" },
      { 0, NULL, NULL }
    };
    GType registered = g_enum_register_static (
        "GstUdpGsoSinkAggregationMode", values);
    g_once_init_leave (&type, registered);
  }

  return (GType) type;
}

#define GST_TYPE_UDP_GSO_SINK_AGGREGATION_MODE \
    (gst_udp_gso_sink_aggregation_mode_get_type ())

static GType
gst_udp_gso_sink_backpressure_get_type (void)
{
  static gsize type = 0;

  if (g_once_init_enter (&type)) {
    static const GEnumValue values[] = {
      { GST_UDP_GSO_SINK_BACKPRESSURE_BLOCK,
        "Block the streaming thread until queue space is available", "block" },
      { GST_UDP_GSO_SINK_BACKPRESSURE_DROP_NEWEST,
        "Drop the incoming datagram when the queue is full", "drop-newest" },
      { GST_UDP_GSO_SINK_BACKPRESSURE_ERROR,
        "Post an error when the queue is full", "error" },
      { 0, NULL, NULL }
    };
    GType registered = g_enum_register_static (
        "GstUdpGsoSinkBackpressure", values);
    g_once_init_leave (&type, registered);
  }

  return (GType) type;
}

#define GST_TYPE_UDP_GSO_SINK_BACKPRESSURE \
    (gst_udp_gso_sink_backpressure_get_type ())

static GType
gst_udp_gso_sink_pacing_mode_get_type (void)
{
  static gsize type = 0;

  if (g_once_init_enter (&type)) {
    static const GEnumValue values[] = {
      { GST_UDP_GSO_SINK_PACING_NONE, "Do not pace datagrams", "none" },
      { GST_UDP_GSO_SINK_PACING_AUTO,
        "Use SO_TXTIME when available, otherwise userspace pacing", "auto" },
      { GST_UDP_GSO_SINK_PACING_TXTIME,
        "Attach SCM_TXTIME to each datagram", "txtime" },
      { GST_UDP_GSO_SINK_PACING_USERSPACE,
        "Wait in the sender worker before each datagram", "userspace" },
      { 0, NULL, NULL }
    };
    GType registered = g_enum_register_static (
        "GstUdpGsoSinkPacingMode", values);
    g_once_init_leave (&type, registered);
  }

  return (GType) type;
}

#define GST_TYPE_UDP_GSO_SINK_PACING_MODE \
    (gst_udp_gso_sink_pacing_mode_get_type ())

static GType
gst_udp_gso_sink_late_policy_get_type (void)
{
  static gsize type = 0;

  if (g_once_init_enter (&type)) {
    static const GEnumValue values[] = {
      { GST_UDP_GSO_SINK_LATE_SEND,
        "Reschedule late datagrams from the current time", "send" },
      { GST_UDP_GSO_SINK_LATE_DROP,
        "Drop datagrams whose intended transmission time has passed", "drop" },
      { 0, NULL, NULL }
    };
    GType registered = g_enum_register_static (
        "GstUdpGsoSinkLatePolicy", values);
    g_once_init_leave (&type, registered);
  }

  return (GType) type;
}

#define GST_TYPE_UDP_GSO_SINK_LATE_POLICY \
    (gst_udp_gso_sink_late_policy_get_type ())

static void
gst_udp_gso_sink_stats_add (GstUdpGsoSink *self,
                         guint64 packets,
                         guint64 bytes,
                         guint64 calls,
                         guint64 sendmmsg_batches,
                         guint64 gso_batches,
                         guint64 gso_segments,
                         guint64 fallbacks)
{
  g_mutex_lock (&self->stats_lock);
  self->stats.packets_sent += packets;
  self->stats.bytes_sent += bytes;
  self->stats.system_calls += calls;
  self->stats.sendmmsg_batches += sendmmsg_batches;
  self->stats.gso_batches += gso_batches;
  self->stats.gso_segments += gso_segments;
  self->stats.fallback_count += fallbacks;
  g_mutex_unlock (&self->stats_lock);
}

static GstUdpGsoSinkStats
gst_udp_gso_sink_stats_snapshot (GstUdpGsoSink *self)
{
  GstUdpGsoSinkStats result;

  g_mutex_lock (&self->stats_lock);
  result = self->stats;
  g_mutex_unlock (&self->stats_lock);

  return result;
}

static void
gst_udp_gso_sink_stats_queue (GstUdpGsoSink *self,
                              guint64 dropped,
                              guint64 late,
                              guint64 high_watermark,
                              guint64 pacing_fallbacks)
{
  g_mutex_lock (&self->stats_lock);
  self->stats.dropped_packets += dropped;
  self->stats.late_packets += late;
  self->stats.queue_high_watermark = MAX (
      self->stats.queue_high_watermark, high_watermark);
  self->stats.pacing_fallbacks += pacing_fallbacks;
  g_mutex_unlock (&self->stats_lock);
}

static void
gst_udp_gso_sink_stats_rtp (GstUdpGsoSink *self,
                            guint64 packets,
                            guint64 invalid,
                            guint64 sequence_gaps,
                            guint64 frame_boundaries)
{
  g_mutex_lock (&self->stats_lock);
  self->stats.rtp_packets += packets;
  self->stats.rtp_invalid_packets += invalid;
  self->stats.rtp_sequence_gaps += sequence_gaps;
  self->stats.rtp_frame_boundaries += frame_boundaries;
  g_mutex_unlock (&self->stats_lock);
}

static guint64
gst_udp_gso_sink_monotonic_time_ns (void)
{
  struct timespec now;

  if (clock_gettime (CLOCK_MONOTONIC, &now) < 0)
    return (guint64) g_get_monotonic_time () * 1000;

  return ((guint64) now.tv_sec * GST_SECOND) + now.tv_nsec;
}

static UdpGsoRtpInfo
gst_udp_gso_sink_parse_rtp (GstBuffer *buffer)
{
  guint8 header[12];
  UdpGsoRtpInfo info = { 0 };
  gsize size = gst_buffer_get_size (buffer);
  guint csrc_count;

  if (size < sizeof (header) ||
      gst_buffer_extract (buffer, 0, header, sizeof (header)) !=
          sizeof (header) ||
      (header[0] >> 6) != 2)
    return info;

  csrc_count = header[0] & 0x0f;
  if (size < sizeof (header) + (csrc_count * 4))
    return info;

  info.valid = true;
  info.marker = (header[1] & 0x80) != 0;
  info.sequence = ((guint16) header[2] << 8) | header[3];
  info.timestamp = ((guint32) header[4] << 24) |
      ((guint32) header[5] << 16) |
      ((guint32) header[6] << 8) | header[7];
  return info;
}

static void
gst_udp_gso_sink_queued_packet_free (GstUdpGsoSinkQueuedPacket *packet)
{
  if (packet == NULL)
    return;
  gst_buffer_unref (packet->buffer);
  g_free (packet);
}

static void
gst_udp_gso_sink_mapped_packet_clear (GstUdpGsoSinkMappedPacket *packet)
{
  guint i;

  if (packet == NULL)
    return;

  for (i = 0; i < packet->n_iov; i++) {
    if (packet->maps[i].memory != NULL)
      gst_memory_unmap (packet->maps[i].memory, &packet->maps[i]);
  }

  g_clear_pointer (&packet->maps, g_free);
  g_clear_pointer (&packet->iov, g_free);
  packet->n_iov = 0;
  packet->size = 0;
}

static gboolean
gst_udp_gso_sink_map_packet (GstUdpGsoSink *self,
                          GstBuffer *buffer,
                          GstUdpGsoSinkMappedPacket *packet)
{
  guint i;

  packet->n_iov = gst_buffer_n_memory (buffer);
  packet->size = 0;

  if (packet->n_iov == 0)
    return TRUE;

  if ((long) packet->n_iov > self->iov_max) {
    GST_ERROR_OBJECT (self, "buffer has %u memory blocks, exceeding IOV_MAX=%ld",
        packet->n_iov, self->iov_max);
    return FALSE;
  }

  packet->maps = g_new0 (GstMapInfo, packet->n_iov);
  packet->iov = g_new0 (struct iovec, packet->n_iov);

  for (i = 0; i < packet->n_iov; i++) {
    GstMemory *memory = gst_buffer_peek_memory (buffer, i);

    if (!gst_memory_map (memory, &packet->maps[i], GST_MAP_READ)) {
      GST_ERROR_OBJECT (self, "failed to map GstMemory %u for reading", i);
      gst_udp_gso_sink_mapped_packet_clear (packet);
      return FALSE;
    }

    packet->iov[i].iov_base = packet->maps[i].data;
    packet->iov[i].iov_len = packet->maps[i].size;
    packet->size += packet->maps[i].size;
  }

  return TRUE;
}

static gboolean
gst_udp_gso_sink_map_buffers (GstUdpGsoSink *self,
                           GstBuffer **buffers,
                           guint count,
                           GstUdpGsoSinkMappedPacket *packets)
{
  guint i;

  for (i = 0; i < count; i++) {
    if (!gst_udp_gso_sink_map_packet (self, buffers[i], &packets[i])) {
      while (i > 0)
        gst_udp_gso_sink_mapped_packet_clear (&packets[--i]);
      return FALSE;
    }
  }

  return TRUE;
}

static void
gst_udp_gso_sink_unmap_packets (GstUdpGsoSinkMappedPacket *packets, guint count)
{
  guint i;

  for (i = 0; i < count; i++)
    gst_udp_gso_sink_mapped_packet_clear (&packets[i]);
}

static GstFlowReturn
gst_udp_gso_sink_report_send_error (GstUdpGsoSink *self,
                                 const gchar *operation,
                                 gint saved_errno)
{
  GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
      ("UDP transmission failed during %s", operation),
      ("%s", g_strerror (saved_errno)));
  return GST_FLOW_ERROR;
}

static ssize_t
gst_udp_gso_sink_sendmsg_retry (GstUdpGsoSink *self,
                             struct msghdr *message,
                             guint64 *attempts)
{
  ssize_t result;

  do {
    (*attempts)++;
    result = sendmsg (self->fd, message, MSG_NOSIGNAL);
  } while (result < 0 && errno == EINTR);

  return result;
}

static gboolean
gst_udp_gso_sink_txtime_fallback_errno (gint value)
{
  return value == EINVAL || value == ENOPROTOOPT || value == EOPNOTSUPP ||
      value == EPERM;
}

static void
gst_udp_gso_sink_disable_txtime (GstUdpGsoSink *self, gint saved_errno)
{
  self->txtime_active = FALSE;
  g_atomic_int_set (&self->txtime_supported, FALSE);
  gst_udp_gso_sink_stats_queue (self, 0, 0, 0, 1);
  if (!self->txtime_warning_emitted) {
    GST_WARNING_OBJECT (self,
        "SCM_TXTIME failed (%s); continuing with userspace pacing",
        g_strerror (saved_errno));
    self->txtime_warning_emitted = TRUE;
  }
}

static GstFlowReturn
gst_udp_gso_sink_wait_until (GstUdpGsoSink *self, guint64 txtime_ns)
{
  gint64 deadline_us = txtime_ns / 1000 > G_MAXINT64 ?
      G_MAXINT64 : (gint64) (txtime_ns / 1000);
  GstFlowReturn flow = GST_FLOW_OK;

  g_mutex_lock (&self->queue_lock);
  while (!self->worker_stop && !self->unlocked &&
      g_get_monotonic_time () < deadline_us)
    g_cond_wait_until (&self->queue_cond, &self->queue_lock, deadline_us);
  if (self->worker_stop || self->unlocked)
    flow = GST_FLOW_FLUSHING;
  g_mutex_unlock (&self->queue_lock);
  return flow;
}

static GstFlowReturn
gst_udp_gso_sink_send_one (GstUdpGsoSink *self,
                           GstUdpGsoSinkMappedPacket *packet,
                           guint64 txtime_ns)
{
  struct msghdr message = { 0 };
  struct cmsghdr *control_message;
  ssize_t sent;
  guint64 attempts = 0;
  gchar control[CMSG_SPACE (sizeof (txtime_ns))] = { 0 };

  message.msg_iov = packet->iov;
  message.msg_iovlen = packet->n_iov;
  if (txtime_ns != 0) {
    message.msg_control = control;
    message.msg_controllen = sizeof (control);
    control_message = CMSG_FIRSTHDR (&message);
    control_message->cmsg_level = SOL_SOCKET;
    control_message->cmsg_type = SCM_TXTIME;
    control_message->cmsg_len = CMSG_LEN (sizeof (txtime_ns));
    memcpy (CMSG_DATA (control_message), &txtime_ns, sizeof (txtime_ns));
  }

  sent = gst_udp_gso_sink_sendmsg_retry (self, &message, &attempts);
  if (sent < 0) {
    gint saved_errno = errno;

    gst_udp_gso_sink_stats_add (self, 0, 0, attempts, 0, 0, 0, 0);
    if (txtime_ns != 0 && self->fallback &&
        gst_udp_gso_sink_txtime_fallback_errno (saved_errno)) {
      gst_udp_gso_sink_disable_txtime (self, saved_errno);
      GstFlowReturn wait_flow = gst_udp_gso_sink_wait_until (self, txtime_ns);

      if (wait_flow != GST_FLOW_OK)
        return wait_flow;
      return gst_udp_gso_sink_send_one (self, packet, 0);
    }
    return gst_udp_gso_sink_report_send_error (self, "sendmsg", saved_errno);
  }

  if ((gsize) sent != packet->size) {
    gst_udp_gso_sink_stats_add (self, 0, 0, attempts, 0, 0, 0, 0);
    return gst_udp_gso_sink_report_send_error (self, "short sendmsg", EIO);
  }

  gst_udp_gso_sink_stats_add (self, 1, packet->size, attempts, 0, 0, 0, 0);
  return GST_FLOW_OK;
}

static GstFlowReturn
gst_udp_gso_sink_send_batch (GstUdpGsoSink *self,
                             GstUdpGsoSinkMappedPacket *packets,
                             guint count,
                             const guint64 *txtimes_ns)
{
  struct mmsghdr *messages;
  gchar *controls = NULL;
  gsize control_stride = CMSG_SPACE (sizeof (guint64));
  guint completed = 0;
  guint i;

  if (count == 0)
    return GST_FLOW_OK;
  if (count == 1)
    return gst_udp_gso_sink_send_one (self, packets,
        txtimes_ns != NULL ? txtimes_ns[0] : 0);

  messages = g_new0 (struct mmsghdr, count);
  if (txtimes_ns != NULL)
    controls = g_malloc0 (control_stride * count);
  for (i = 0; i < count; i++) {
    struct cmsghdr *control_message;

    messages[i].msg_hdr.msg_iov = packets[i].iov;
    messages[i].msg_hdr.msg_iovlen = packets[i].n_iov;
    if (txtimes_ns == NULL)
      continue;

    messages[i].msg_hdr.msg_control = controls + (control_stride * i);
    messages[i].msg_hdr.msg_controllen = control_stride;
    control_message = CMSG_FIRSTHDR (&messages[i].msg_hdr);
    control_message->cmsg_level = SOL_SOCKET;
    control_message->cmsg_type = SCM_TXTIME;
    control_message->cmsg_len = CMSG_LEN (sizeof (txtimes_ns[i]));
    memcpy (CMSG_DATA (control_message), &txtimes_ns[i],
        sizeof (txtimes_ns[i]));
  }

  while (completed < count) {
    gint result;
    guint64 attempts = 0;
    guint64 batch_bytes = 0;

    do {
      attempts++;
      result = sendmmsg (self->fd, messages + completed, count - completed,
          MSG_NOSIGNAL);
    } while (result < 0 && errno == EINTR);

    if (result < 0) {
      gint saved_errno = errno;
      guint retry_offset = completed;

      gst_udp_gso_sink_stats_add (self, 0, 0, attempts, attempts, 0, 0, 0);
      g_free (controls);
      g_free (messages);
      if (txtimes_ns != NULL && self->fallback &&
          gst_udp_gso_sink_txtime_fallback_errno (saved_errno)) {
        gst_udp_gso_sink_disable_txtime (self, saved_errno);
        for (i = retry_offset; i < count; i++) {
          GstFlowReturn flow;

          GstFlowReturn wait_flow = gst_udp_gso_sink_wait_until (self,
              txtimes_ns[i]);

          if (wait_flow != GST_FLOW_OK)
            return wait_flow;
          flow = gst_udp_gso_sink_send_one (self, &packets[i], 0);
          if (flow != GST_FLOW_OK)
            return flow;
        }
        return GST_FLOW_OK;
      }
      return gst_udp_gso_sink_report_send_error (self, "sendmmsg", saved_errno);
    }

    if (result == 0) {
      gst_udp_gso_sink_stats_add (self, 0, 0, attempts, attempts, 0, 0, 0);
      g_free (controls);
      g_free (messages);
      return gst_udp_gso_sink_report_send_error (self, "zero-length sendmmsg result",
          EIO);
    }

    for (i = 0; i < (guint) result; i++) {
      guint index = completed + i;

      if (messages[index].msg_len != packets[index].size) {
        gst_udp_gso_sink_stats_add (self, i, batch_bytes, attempts, attempts,
            0, 0, 0);
        g_free (controls);
        g_free (messages);
        return gst_udp_gso_sink_report_send_error (self, "short sendmmsg", EIO);
      }
      batch_bytes += packets[index].size;
    }

    gst_udp_gso_sink_stats_add (self, result, batch_bytes, attempts, attempts,
        0, 0, 0);
    completed += result;
  }

  g_free (controls);
  g_free (messages);
  return GST_FLOW_OK;
}

static gboolean
gst_udp_gso_sink_gso_fallback_errno (gint value)
{
  return value == EINVAL || value == ENOPROTOOPT || value == EOPNOTSUPP ||
      value == EMSGSIZE;
}

static guint
gst_udp_gso_sink_limit_run_by_iov (GstUdpGsoSink *self,
                                GstUdpGsoSinkMappedPacket *packets,
                                guint run)
{
  long vectors = 0;
  guint i;

  for (i = 0; i < run; i++) {
    if (vectors + packets[i].n_iov > self->iov_max)
      break;
    vectors += packets[i].n_iov;
  }

  return i;
}

static GstFlowReturn
gst_udp_gso_sink_send_gso (GstUdpGsoSink *self,
                        GstUdpGsoSinkMappedPacket *packets,
                        guint count)
{
  struct msghdr message = { 0 };
  struct cmsghdr *control_message;
  struct iovec *vectors;
  guint16 segment_size;
  guint vector_count = 0;
  guint i;
  guint j;
  gsize expected = 0;
  ssize_t sent;
  guint64 attempts = 0;
  gchar control[CMSG_SPACE (sizeof (segment_size))] = { 0 };

  g_assert (count >= 2);
  g_assert (packets[0].size > 0 && packets[0].size <= G_MAXUINT16);

  for (i = 0; i < count; i++)
    vector_count += packets[i].n_iov;

  vectors = g_new (struct iovec, vector_count);
  vector_count = 0;
  for (i = 0; i < count; i++) {
    expected += packets[i].size;
    for (j = 0; j < packets[i].n_iov; j++)
      vectors[vector_count++] = packets[i].iov[j];
  }

  segment_size = (guint16) packets[0].size;
  message.msg_iov = vectors;
  message.msg_iovlen = vector_count;
  message.msg_control = control;
  message.msg_controllen = sizeof (control);

  control_message = CMSG_FIRSTHDR (&message);
  control_message->cmsg_level = SOL_UDP;
  control_message->cmsg_type = UDP_SEGMENT;
  control_message->cmsg_len = CMSG_LEN (sizeof (segment_size));
  memcpy (CMSG_DATA (control_message), &segment_size, sizeof (segment_size));

  sent = gst_udp_gso_sink_sendmsg_retry (self, &message, &attempts);
  if (sent < 0) {
    gint saved_errno = errno;

    g_free (vectors);
    gst_udp_gso_sink_stats_add (self, 0, 0, attempts, 0, 0, 0, 0);
    if (self->fallback && gst_udp_gso_sink_gso_fallback_errno (saved_errno)) {
      gst_udp_gso_sink_stats_add (self, 0, 0, 0, 0, 0, 0, 1);
      if (saved_errno == ENOPROTOOPT || saved_errno == EOPNOTSUPP)
        g_atomic_int_set (&self->gso_supported, FALSE);
      if (!self->gso_warning_emitted) {
        GST_WARNING_OBJECT (self,
            "UDP GSO failed (%s); falling back to sendmmsg",
            g_strerror (saved_errno));
        self->gso_warning_emitted = TRUE;
      }
      return gst_udp_gso_sink_send_batch (self, packets, count, NULL);
    }
    return gst_udp_gso_sink_report_send_error (self, "UDP_SEGMENT sendmsg",
        saved_errno);
  }

  g_free (vectors);
  if ((gsize) sent != expected) {
    gst_udp_gso_sink_stats_add (self, 0, 0, attempts, 0, 0, 0, 0);
    return gst_udp_gso_sink_report_send_error (self, "short UDP_SEGMENT sendmsg",
        EIO);
  }

  gst_udp_gso_sink_stats_add (self, count, expected, attempts, 0, 1, count, 0);
  return GST_FLOW_OK;
}

static GstFlowReturn
gst_udp_gso_sink_send_with_mode (GstUdpGsoSink *self,
                              GstUdpGsoSinkMappedPacket *packets,
                              guint count)
{
  guint offset = 0;
  size_t *sizes;
  guint i;

  if (self->io_mode == GST_UDP_GSO_SINK_IO_SENDMSG) {
    while (offset < count) {
      GstFlowReturn flow = gst_udp_gso_sink_send_one (self,
          &packets[offset++], 0);
      if (flow != GST_FLOW_OK)
        return flow;
    }
    return GST_FLOW_OK;
  }

  if (self->io_mode == GST_UDP_GSO_SINK_IO_SENDMMSG ||
      !g_atomic_int_get (&self->gso_supported)) {
    while (offset < count) {
      guint batch = MIN (self->batch_size, count - offset);
      GstFlowReturn flow = gst_udp_gso_sink_send_batch (self,
          &packets[offset], batch, NULL);
      if (flow != GST_FLOW_OK)
        return flow;
      offset += batch;
    }
    return GST_FLOW_OK;
  }

  sizes = g_new (size_t, count);
  for (i = 0; i < count; i++)
    sizes[i] = packets[i].size;

  while (offset < count) {
    guint available = MIN (self->gso_max_segments, count - offset);
    guint run;
    guint limited_run;
    GstFlowReturn flow;

    if (!g_atomic_int_get (&self->gso_supported)) {
      while (offset < count) {
        guint batch = MIN (self->batch_size, count - offset);

        flow = gst_udp_gso_sink_send_batch (self, &packets[offset], batch,
            NULL);
        if (flow != GST_FLOW_OK) {
          g_free (sizes);
          return flow;
        }
        offset += batch;
      }
      break;
    }

    run = udpgso_equal_size_run (sizes, count, offset, available);

    limited_run = gst_udp_gso_sink_limit_run_by_iov (self,
        &packets[offset], run);
    run = MIN (run, limited_run);

    if (run >= self->gso_min_segments &&
        packets[offset].size <= G_MAXUINT16) {
      flow = gst_udp_gso_sink_send_gso (self, &packets[offset], run);
      if (flow != GST_FLOW_OK) {
        g_free (sizes);
        return flow;
      }
      offset += run;
      continue;
    }

    /* Batch the incompatible tail until the next potentially useful GSO run. */
    run = 1;
    while (offset + run < count && run < self->batch_size) {
      guint remaining = MIN (self->gso_max_segments, count - offset - run);
      guint candidate;

      candidate = udpgso_equal_size_run (sizes, count, offset + run,
          remaining);
      if (candidate >= self->gso_min_segments)
        break;
      run++;
    }

    flow = gst_udp_gso_sink_send_batch (self, &packets[offset], run, NULL);
    if (flow != GST_FLOW_OK) {
      g_free (sizes);
      return flow;
    }
    offset += run;
  }

  g_free (sizes);
  return GST_FLOW_OK;
}

static GstFlowReturn
gst_udp_gso_sink_send_paced (GstUdpGsoSink *self,
                             GstUdpGsoSinkMappedPacket *packets,
                             const guint64 *txtimes_ns,
                             guint count)
{
  guint offset = 0;

  while (offset < count) {
    GstFlowReturn flow;

    if (!self->txtime_active ||
        self->io_mode == GST_UDP_GSO_SINK_IO_SENDMSG) {
      if (!self->txtime_active) {
        GstFlowReturn wait_flow = gst_udp_gso_sink_wait_until (self,
            txtimes_ns[offset]);

        if (wait_flow != GST_FLOW_OK)
          return wait_flow;
      }
      flow = gst_udp_gso_sink_send_one (self, &packets[offset],
          self->txtime_active ? txtimes_ns[offset] : 0);
      offset++;
    } else {
      guint batch = MIN (self->batch_size, count - offset);

      flow = gst_udp_gso_sink_send_batch (self, &packets[offset], batch,
          &txtimes_ns[offset]);
      offset += batch;
    }

    if (flow != GST_FLOW_OK)
      return flow;
  }

  return GST_FLOW_OK;
}

static GstFlowReturn
gst_udp_gso_sink_send_packets (GstUdpGsoSink *self,
                               GstUdpGsoSinkMappedPacket *packets,
                               const guint64 *txtimes_ns,
                               guint count)
{
  if (txtimes_ns != NULL)
    return gst_udp_gso_sink_send_paced (self, packets, txtimes_ns, count);
  return gst_udp_gso_sink_send_with_mode (self, packets, count);
}

static gboolean
gst_udp_gso_sink_probe_gso (GstUdpGsoSink *self)
{
  gint disabled = 0;

  if (setsockopt (self->fd, SOL_UDP, UDP_SEGMENT, &disabled,
          sizeof (disabled)) == 0)
    return TRUE;

  GST_INFO_OBJECT (self, "UDP_SEGMENT is unavailable: %s", g_strerror (errno));
  return FALSE;
}

static gboolean
gst_udp_gso_sink_configure_txtime (GstUdpGsoSink *self)
{
  struct sock_txtime socket_txtime = {
    .clockid = CLOCK_MONOTONIC,
    .flags = 0,
  };

  self->txtime_active = FALSE;
  g_atomic_int_set (&self->txtime_supported, FALSE);
  if (self->pacing_mode == GST_UDP_GSO_SINK_PACING_NONE ||
      self->pacing_mode == GST_UDP_GSO_SINK_PACING_USERSPACE)
    return TRUE;

  if (setsockopt (self->fd, SOL_SOCKET, SO_TXTIME, &socket_txtime,
          sizeof (socket_txtime)) == 0) {
    self->txtime_active = TRUE;
    g_atomic_int_set (&self->txtime_supported, TRUE);
    return TRUE;
  }

  if (self->pacing_mode == GST_UDP_GSO_SINK_PACING_AUTO || self->fallback) {
    gst_udp_gso_sink_disable_txtime (self, errno);
    return TRUE;
  }

  GST_ELEMENT_ERROR (self, RESOURCE, SETTINGS,
      ("SO_TXTIME was requested but is unavailable"),
      ("%s", g_strerror (errno)));
  return FALSE;
}

static void
gst_udp_gso_sink_record_rtp (GstUdpGsoSink *self,
                             const UdpGsoRtpInfo *rtp)
{
  guint64 invalid = 0;
  guint64 sequence_gaps = 0;
  guint64 frame_boundaries = 0;

  if (!self->rtp_aware)
    return;

  g_mutex_lock (&self->queue_lock);
  if (!rtp->valid) {
    invalid = 1;
  } else {
    if (self->last_rtp_valid) {
      if (rtp->sequence != (guint16) (self->last_rtp_sequence + 1))
        sequence_gaps = 1;
      if (self->last_rtp_marker ||
          self->last_rtp_timestamp != rtp->timestamp)
        frame_boundaries = 1;
    }
    self->last_rtp_valid = TRUE;
    self->last_rtp_sequence = rtp->sequence;
    self->last_rtp_timestamp = rtp->timestamp;
    self->last_rtp_marker = rtp->marker;
  }
  g_mutex_unlock (&self->queue_lock);

  gst_udp_gso_sink_stats_rtp (self, rtp->valid ? 1 : 0, invalid,
      sequence_gaps, frame_boundaries);
}

static GstUdpGsoSinkQueuedPacket *
gst_udp_gso_sink_queued_packet_new (GstUdpGsoSink *self, GstBuffer *buffer)
{
  GstUdpGsoSinkQueuedPacket *packet = g_new0 (GstUdpGsoSinkQueuedPacket, 1);

  packet->buffer = gst_buffer_ref (buffer);
  packet->enqueue_time_us = g_get_monotonic_time ();
  if (self->rtp_aware) {
    packet->rtp = gst_udp_gso_sink_parse_rtp (buffer);
    gst_udp_gso_sink_record_rtp (self, &packet->rtp);
  }
  return packet;
}

static GstFlowReturn
gst_udp_gso_sink_transmit_queued (GstUdpGsoSink *self,
                                 GstUdpGsoSinkQueuedPacket **queued,
                                 guint count)
{
  GstUdpGsoSinkMappedPacket *packets;
  GstBuffer **buffers;
  guint64 *txtimes = NULL;
  guint active = 0;
  guint64 late = 0;
  guint64 dropped = 0;
  GstFlowReturn flow;
  guint i;

  if (count == 0)
    return GST_FLOW_OK;

  buffers = g_new (GstBuffer *, count);
  if (self->pacing_mode != GST_UDP_GSO_SINK_PACING_NONE)
    txtimes = g_new (guint64, count);

  for (i = 0; i < count; i++) {
    if (txtimes != NULL) {
      guint64 txtime = udpgso_schedule_next (&self->schedule,
          ((guint64) queued[i]->enqueue_time_us) * 1000,
          gst_buffer_get_size (queued[i]->buffer));
      guint64 now = gst_udp_gso_sink_monotonic_time_ns ();

      if (self->txtime_active && self->max_pacing_horizon_us != 0 &&
          txtime > now +
              (((guint64) self->max_pacing_horizon_us) * 1000)) {
        GstFlowReturn wait_flow = gst_udp_gso_sink_wait_until (self,
            txtime - (((guint64) self->max_pacing_horizon_us) * 1000));

        if (wait_flow != GST_FLOW_OK) {
          g_free (txtimes);
          g_free (buffers);
          return wait_flow;
        }
        now = gst_udp_gso_sink_monotonic_time_ns ();
      }

      if (txtime <= now) {
        late++;
        if (self->late_packet_policy == GST_UDP_GSO_SINK_LATE_DROP) {
          dropped++;
          continue;
        }

        udpgso_schedule_reset (&self->schedule);
        txtime = udpgso_schedule_next (&self->schedule, now,
            gst_buffer_get_size (queued[i]->buffer));
      }
      txtimes[active] = txtime;
    }

    buffers[active++] = queued[i]->buffer;
  }

  if (late != 0 || dropped != 0)
    gst_udp_gso_sink_stats_queue (self, dropped, late, 0, 0);
  if (active == 0) {
    g_free (txtimes);
    g_free (buffers);
    return GST_FLOW_OK;
  }

  packets = g_new0 (GstUdpGsoSinkMappedPacket, active);
  if (!gst_udp_gso_sink_map_buffers (self, buffers, active, packets)) {
    g_free (packets);
    g_free (txtimes);
    g_free (buffers);
    return GST_FLOW_ERROR;
  }

  flow = gst_udp_gso_sink_send_packets (self, packets, txtimes, active);
  gst_udp_gso_sink_unmap_packets (packets, active);
  g_free (packets);
  g_free (txtimes);
  g_free (buffers);
  return flow;
}

static void
gst_udp_gso_sink_clear_queue_locked (GstUdpGsoSink *self,
                                     gboolean record_drops)
{
  guint64 dropped = 0;
  GstUdpGsoSinkQueuedPacket *packet;

  while ((packet = g_queue_pop_head (&self->packet_queue)) != NULL) {
    gst_udp_gso_sink_queued_packet_free (packet);
    dropped++;
  }
  if (record_drops && dropped != 0)
    gst_udp_gso_sink_stats_queue (self, dropped, 0, 0, 0);
  g_cond_broadcast (&self->queue_cond);
  g_cond_broadcast (&self->drain_cond);
}

static guint
gst_udp_gso_sink_batch_count_locked (GstUdpGsoSink *self)
{
  guint available = MIN (self->batch_size,
      (guint) self->packet_queue.length);
  GstUdpGsoSinkQueuedPacket *previous = NULL;
  guint count;

  for (count = 0; count < available; count++) {
    GstUdpGsoSinkQueuedPacket *packet = g_queue_peek_nth (
        &self->packet_queue, count);

    if (count > 0 && self->rtp_aware &&
        udpgso_rtp_starts_new_batch (&previous->rtp, &packet->rtp))
      break;
    previous = packet;
  }

  return MAX (count, 1);
}

static gpointer
gst_udp_gso_sink_worker (gpointer data)
{
  GstUdpGsoSink *self = data;

  g_mutex_lock (&self->queue_lock);
  while (!self->worker_stop) {
    GstUdpGsoSinkQueuedPacket **batch;
    GstUdpGsoSinkQueuedPacket *first;
    GstFlowReturn flow;
    gint64 deadline;
    guint count;
    guint i;

    while (!self->worker_stop && g_queue_is_empty (&self->packet_queue))
      g_cond_wait (&self->queue_cond, &self->queue_lock);
    if (self->worker_stop)
      break;

    first = g_queue_peek_head (&self->packet_queue);
    deadline = first->enqueue_time_us + self->max_batch_delay_us;
    while (!self->worker_stop && !self->worker_flush) {
      count = gst_udp_gso_sink_batch_count_locked (self);
      if (count >= self->batch_size ||
          count < self->packet_queue.length ||
          self->max_batch_delay_us == 0 ||
          g_get_monotonic_time () >= deadline)
        break;
      g_cond_wait_until (&self->queue_cond, &self->queue_lock, deadline);
    }
    if (self->worker_stop)
      break;

    count = gst_udp_gso_sink_batch_count_locked (self);
    batch = g_new (GstUdpGsoSinkQueuedPacket *, count);
    for (i = 0; i < count; i++)
      batch[i] = g_queue_pop_head (&self->packet_queue);
    self->worker_busy = TRUE;
    g_cond_broadcast (&self->queue_cond);
    g_mutex_unlock (&self->queue_lock);

    flow = gst_udp_gso_sink_transmit_queued (self, batch, count);
    for (i = 0; i < count; i++)
      gst_udp_gso_sink_queued_packet_free (batch[i]);
    g_free (batch);

    g_mutex_lock (&self->queue_lock);
    self->worker_busy = FALSE;
    if (flow != GST_FLOW_OK) {
      if (!(flow == GST_FLOW_FLUSHING && self->unlocked))
        self->worker_flow = flow;
      gst_udp_gso_sink_clear_queue_locked (self, TRUE);
    }
    if (g_queue_is_empty (&self->packet_queue))
      g_cond_broadcast (&self->drain_cond);
  }

  self->worker_busy = FALSE;
  g_cond_broadcast (&self->queue_cond);
  g_cond_broadcast (&self->drain_cond);
  g_mutex_unlock (&self->queue_lock);
  return NULL;
}

static GstFlowReturn
gst_udp_gso_sink_enqueue (GstUdpGsoSink *self,
                          GstUdpGsoSinkQueuedPacket *packet)
{
  GstFlowReturn flow = GST_FLOW_OK;
  gboolean queue_error = FALSE;
  guint64 queue_length = 0;

  g_mutex_lock (&self->queue_lock);
  while (!self->worker_stop && !self->unlocked &&
      self->worker_flow == GST_FLOW_OK &&
      self->packet_queue.length >= self->max_queue_packets &&
      self->backpressure == GST_UDP_GSO_SINK_BACKPRESSURE_BLOCK)
    g_cond_wait (&self->queue_cond, &self->queue_lock);

  if (self->worker_stop || self->unlocked) {
    flow = GST_FLOW_FLUSHING;
  } else if (self->worker_flow != GST_FLOW_OK) {
    flow = self->worker_flow;
  } else if (self->packet_queue.length >= self->max_queue_packets) {
    if (self->backpressure == GST_UDP_GSO_SINK_BACKPRESSURE_DROP_NEWEST) {
      gst_udp_gso_sink_stats_queue (self, 1, 0, 0, 0);
    } else {
      queue_error = TRUE;
      flow = GST_FLOW_ERROR;
    }
  } else {
    g_queue_push_tail (&self->packet_queue, packet);
    packet = NULL;
    queue_length = self->packet_queue.length;
    g_cond_signal (&self->queue_cond);
  }
  g_mutex_unlock (&self->queue_lock);

  gst_udp_gso_sink_queued_packet_free (packet);
  if (queue_length != 0)
    gst_udp_gso_sink_stats_queue (self, 0, 0, queue_length, 0);
  if (queue_error) {
    GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
        ("UDP sender queue reached its configured limit of %u packets",
            self->max_queue_packets),
        ("Select backpressure=block or drop-newest, or increase "
            "max-queue-packets"));
  }
  return flow;
}

static gboolean
gst_udp_gso_sink_drain (GstUdpGsoSink *self)
{
  gboolean success;

  if (self->aggregation_mode != GST_UDP_GSO_SINK_AGGREGATION_BOUNDED)
    return TRUE;

  g_mutex_lock (&self->queue_lock);
  self->worker_flush = TRUE;
  g_cond_broadcast (&self->queue_cond);
  while ((!g_queue_is_empty (&self->packet_queue) || self->worker_busy) &&
      self->worker_flow == GST_FLOW_OK && !self->worker_stop &&
      !self->unlocked)
    g_cond_wait (&self->drain_cond, &self->queue_lock);
  success = self->worker_flow == GST_FLOW_OK && !self->worker_stop &&
      !self->unlocked;
  self->worker_flush = FALSE;
  g_mutex_unlock (&self->queue_lock);
  return success;
}

static gboolean
gst_udp_gso_sink_start (GstBaseSink *base_sink)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (base_sink);
  struct addrinfo hints = { 0 };
  struct addrinfo *addresses = NULL;
  struct addrinfo *address;
  gchar port_string[16];
  gint lookup_result;

  if (self->gso_min_segments > self->gso_max_segments) {
    GST_ELEMENT_ERROR (self, RESOURCE, SETTINGS,
        ("gso-min-segments must not exceed gso-max-segments"), (NULL));
    return FALSE;
  }
  if (self->pacing_mode != GST_UDP_GSO_SINK_PACING_NONE &&
      self->pacing_rate == 0) {
    GST_ELEMENT_ERROR (self, RESOURCE, SETTINGS,
        ("pacing-rate must be greater than zero when pacing is enabled"),
        (NULL));
    return FALSE;
  }

  g_snprintf (port_string, sizeof (port_string), "%u", self->port);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_protocol = IPPROTO_UDP;

  lookup_result = getaddrinfo (self->host, port_string, &hints, &addresses);
  if (lookup_result != 0) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("Could not resolve UDP destination %s:%u", self->host, self->port),
        ("%s", gai_strerror (lookup_result)));
    return FALSE;
  }

  self->fd = -1;
  for (address = addresses; address != NULL; address = address->ai_next) {
    self->fd = socket (address->ai_family,
        address->ai_socktype | SOCK_CLOEXEC, address->ai_protocol);
    if (self->fd < 0)
      continue;

    if (self->send_buffer_size > 0 &&
        setsockopt (self->fd, SOL_SOCKET, SO_SNDBUF, &self->send_buffer_size,
            sizeof (self->send_buffer_size)) < 0) {
      GST_WARNING_OBJECT (self, "could not set SO_SNDBUF=%d: %s",
          self->send_buffer_size, g_strerror (errno));
    }

    if (connect (self->fd, address->ai_addr, address->ai_addrlen) == 0)
      break;

    close (self->fd);
    self->fd = -1;
  }
  freeaddrinfo (addresses);

  if (self->fd < 0) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("Could not connect UDP socket to %s:%u", self->host, self->port),
        ("%s", g_strerror (errno)));
    return FALSE;
  }

  g_atomic_int_set (&self->gso_supported, gst_udp_gso_sink_probe_gso (self));
  self->gso_warning_emitted = FALSE;
  self->txtime_warning_emitted = FALSE;
  if (self->io_mode == GST_UDP_GSO_SINK_IO_GSO &&
      !g_atomic_int_get (&self->gso_supported) && !self->fallback) {
    GST_ELEMENT_ERROR (self, RESOURCE, SETTINGS,
        ("UDP GSO was requested but is unavailable"),
        ("Enable fallback or select another io-mode"));
    close (self->fd);
    self->fd = -1;
    return FALSE;
  }

  if (!gst_udp_gso_sink_configure_txtime (self)) {
    close (self->fd);
    self->fd = -1;
    return FALSE;
  }
  if (self->pacing_mode != GST_UDP_GSO_SINK_PACING_NONE &&
      (self->io_mode == GST_UDP_GSO_SINK_IO_GSO ||
          self->io_mode == GST_UDP_GSO_SINK_IO_AUTO)) {
    GST_INFO_OBJECT (self,
        "per-packet pacing uses timed sendmmsg submissions; UDP GSO is "
        "not used while pacing is enabled");
  }

  self->schedule.rate_bps = self->pacing_rate;
  self->schedule.lead_time_ns =
      ((guint64) self->pacing_lead_time_us) * 1000;
  self->schedule.wire_overhead_bytes = self->wire_overhead_bytes;
  udpgso_schedule_reset (&self->schedule);

  g_mutex_lock (&self->queue_lock);
  gst_udp_gso_sink_clear_queue_locked (self, FALSE);
  self->worker_stop = FALSE;
  self->worker_flush = FALSE;
  self->worker_busy = FALSE;
  self->unlocked = FALSE;
  self->worker_flow = GST_FLOW_OK;
  self->last_rtp_valid = FALSE;
  g_mutex_unlock (&self->queue_lock);
  if (self->aggregation_mode == GST_UDP_GSO_SINK_AGGREGATION_BOUNDED)
    self->worker = g_thread_new ("udpgsosink-sender",
        gst_udp_gso_sink_worker, self);

  GST_INFO_OBJECT (self,
      "sending to %s:%u with io-mode=%d, aggregation=%d, GSO=%s, pacing=%d",
      self->host, self->port, self->io_mode,
      self->aggregation_mode,
      g_atomic_int_get (&self->gso_supported) ? "available" : "unavailable",
      self->pacing_mode);
  return TRUE;
}

static gboolean
gst_udp_gso_sink_unlock (GstBaseSink *base_sink)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (base_sink);

  g_mutex_lock (&self->queue_lock);
  self->unlocked = TRUE;
  gst_udp_gso_sink_clear_queue_locked (self, TRUE);
  g_cond_broadcast (&self->queue_cond);
  g_cond_broadcast (&self->drain_cond);
  g_mutex_unlock (&self->queue_lock);
  return TRUE;
}

static gboolean
gst_udp_gso_sink_unlock_stop (GstBaseSink *base_sink)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (base_sink);

  g_mutex_lock (&self->queue_lock);
  self->unlocked = FALSE;
  udpgso_schedule_reset (&self->schedule);
  g_mutex_unlock (&self->queue_lock);
  return TRUE;
}

static gboolean
gst_udp_gso_sink_stop (GstBaseSink *base_sink)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (base_sink);
  GThread *worker = NULL;

  g_mutex_lock (&self->queue_lock);
  self->worker_stop = TRUE;
  if (self->worker != NULL) {
    gst_udp_gso_sink_clear_queue_locked (self, TRUE);
    g_cond_broadcast (&self->queue_cond);
    worker = self->worker;
    self->worker = NULL;
  }
  g_mutex_unlock (&self->queue_lock);
  if (worker != NULL)
    g_thread_join (worker);

  if (self->fd >= 0) {
    close (self->fd);
    self->fd = -1;
  }
  g_atomic_int_set (&self->gso_supported, FALSE);
  g_atomic_int_set (&self->txtime_supported, FALSE);
  self->txtime_active = FALSE;
  return TRUE;
}

static gboolean
gst_udp_gso_sink_event (GstBaseSink *base_sink, GstEvent *event)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (base_sink);

  switch (GST_EVENT_TYPE (event)) {
    case GST_EVENT_EOS:
      if (!gst_udp_gso_sink_drain (self))
        return FALSE;
      break;
    case GST_EVENT_FLUSH_START:
      g_mutex_lock (&self->queue_lock);
      self->worker_flush = TRUE;
      gst_udp_gso_sink_clear_queue_locked (self, TRUE);
      udpgso_schedule_reset (&self->schedule);
      self->last_rtp_valid = FALSE;
      g_mutex_unlock (&self->queue_lock);
      break;
    case GST_EVENT_FLUSH_STOP:
      g_mutex_lock (&self->queue_lock);
      self->worker_flush = FALSE;
      udpgso_schedule_reset (&self->schedule);
      self->last_rtp_valid = FALSE;
      g_mutex_unlock (&self->queue_lock);
      break;
    default:
      break;
  }

  return GST_BASE_SINK_CLASS (gst_udp_gso_sink_parent_class)->event (
      base_sink, event);
}

static GstFlowReturn
gst_udp_gso_sink_render (GstBaseSink *base_sink, GstBuffer *buffer)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (base_sink);
  GstUdpGsoSinkQueuedPacket *packet;
  GstFlowReturn flow;

  packet = gst_udp_gso_sink_queued_packet_new (self, buffer);
  if (self->aggregation_mode == GST_UDP_GSO_SINK_AGGREGATION_BOUNDED)
    return gst_udp_gso_sink_enqueue (self, packet);

  /* One GstBuffer is one UDP datagram; it must never be segmented implicitly. */
  flow = gst_udp_gso_sink_transmit_queued (self, &packet, 1);
  gst_udp_gso_sink_queued_packet_free (packet);
  return flow;
}

static GstFlowReturn
gst_udp_gso_sink_render_list (GstBaseSink *base_sink, GstBufferList *buffer_list)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (base_sink);
  GstUdpGsoSinkQueuedPacket **packets;
  GstFlowReturn flow;
  guint count;
  guint i;

  count = gst_buffer_list_length (buffer_list);
  if (count == 0)
    return GST_FLOW_OK;

  if (self->aggregation_mode == GST_UDP_GSO_SINK_AGGREGATION_BOUNDED) {
    for (i = 0; i < count; i++) {
      GstUdpGsoSinkQueuedPacket *packet = gst_udp_gso_sink_queued_packet_new (
          self, gst_buffer_list_get (buffer_list, i));

      flow = gst_udp_gso_sink_enqueue (self, packet);
      if (flow != GST_FLOW_OK)
        return flow;
    }
    return GST_FLOW_OK;
  }

  packets = g_new (GstUdpGsoSinkQueuedPacket *, count);
  for (i = 0; i < count; i++)
    packets[i] = gst_udp_gso_sink_queued_packet_new (self,
        gst_buffer_list_get (buffer_list, i));
  flow = gst_udp_gso_sink_transmit_queued (self, packets, count);
  for (i = 0; i < count; i++)
    gst_udp_gso_sink_queued_packet_free (packets[i]);
  g_free (packets);
  return flow;
}

static void
gst_udp_gso_sink_set_property (GObject *object,
                            guint property_id,
                            const GValue *value,
                            GParamSpec *pspec)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (object);

  switch (property_id) {
    case PROP_HOST:
      g_free (self->host);
      self->host = g_value_dup_string (value);
      if (self->host == NULL)
        self->host = g_strdup (DEFAULT_HOST);
      break;
    case PROP_PORT:
      self->port = g_value_get_uint (value);
      break;
    case PROP_IO_MODE:
      self->io_mode = g_value_get_enum (value);
      break;
    case PROP_BATCH_SIZE:
      self->batch_size = g_value_get_uint (value);
      break;
    case PROP_GSO_MIN_SEGMENTS:
      self->gso_min_segments = g_value_get_uint (value);
      break;
    case PROP_GSO_MAX_SEGMENTS:
      self->gso_max_segments = g_value_get_uint (value);
      break;
    case PROP_FALLBACK:
      self->fallback = g_value_get_boolean (value);
      break;
    case PROP_SEND_BUFFER_SIZE:
      self->send_buffer_size = g_value_get_int (value);
      break;
    case PROP_AGGREGATION_MODE:
      self->aggregation_mode = g_value_get_enum (value);
      break;
    case PROP_MAX_QUEUE_PACKETS:
      self->max_queue_packets = g_value_get_uint (value);
      break;
    case PROP_MAX_BATCH_DELAY_US:
      self->max_batch_delay_us = g_value_get_uint (value);
      break;
    case PROP_BACKPRESSURE:
      self->backpressure = g_value_get_enum (value);
      break;
    case PROP_PACING_MODE:
      self->pacing_mode = g_value_get_enum (value);
      break;
    case PROP_PACING_RATE:
      self->pacing_rate = g_value_get_uint64 (value);
      break;
    case PROP_PACING_LEAD_TIME_US:
      self->pacing_lead_time_us = g_value_get_uint (value);
      break;
    case PROP_MAX_PACING_HORIZON_US:
      self->max_pacing_horizon_us = g_value_get_uint (value);
      break;
    case PROP_WIRE_OVERHEAD_BYTES:
      self->wire_overhead_bytes = g_value_get_uint (value);
      break;
    case PROP_LATE_PACKET_POLICY:
      self->late_packet_policy = g_value_get_enum (value);
      break;
    case PROP_RTP_AWARE:
      self->rtp_aware = g_value_get_boolean (value);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
  }
}

static void
gst_udp_gso_sink_get_property (GObject *object,
                            guint property_id,
                            GValue *value,
                            GParamSpec *pspec)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (object);
  GstUdpGsoSinkStats stats = { 0 };

  if (property_id >= PROP_PACKETS_SENT)
    stats = gst_udp_gso_sink_stats_snapshot (self);

  switch (property_id) {
    case PROP_HOST:
      g_value_set_string (value, self->host);
      break;
    case PROP_PORT:
      g_value_set_uint (value, self->port);
      break;
    case PROP_IO_MODE:
      g_value_set_enum (value, self->io_mode);
      break;
    case PROP_BATCH_SIZE:
      g_value_set_uint (value, self->batch_size);
      break;
    case PROP_GSO_MIN_SEGMENTS:
      g_value_set_uint (value, self->gso_min_segments);
      break;
    case PROP_GSO_MAX_SEGMENTS:
      g_value_set_uint (value, self->gso_max_segments);
      break;
    case PROP_FALLBACK:
      g_value_set_boolean (value, self->fallback);
      break;
    case PROP_SEND_BUFFER_SIZE:
      g_value_set_int (value, self->send_buffer_size);
      break;
    case PROP_AGGREGATION_MODE:
      g_value_set_enum (value, self->aggregation_mode);
      break;
    case PROP_MAX_QUEUE_PACKETS:
      g_value_set_uint (value, self->max_queue_packets);
      break;
    case PROP_MAX_BATCH_DELAY_US:
      g_value_set_uint (value, self->max_batch_delay_us);
      break;
    case PROP_BACKPRESSURE:
      g_value_set_enum (value, self->backpressure);
      break;
    case PROP_PACING_MODE:
      g_value_set_enum (value, self->pacing_mode);
      break;
    case PROP_PACING_RATE:
      g_value_set_uint64 (value, self->pacing_rate);
      break;
    case PROP_PACING_LEAD_TIME_US:
      g_value_set_uint (value, self->pacing_lead_time_us);
      break;
    case PROP_MAX_PACING_HORIZON_US:
      g_value_set_uint (value, self->max_pacing_horizon_us);
      break;
    case PROP_WIRE_OVERHEAD_BYTES:
      g_value_set_uint (value, self->wire_overhead_bytes);
      break;
    case PROP_LATE_PACKET_POLICY:
      g_value_set_enum (value, self->late_packet_policy);
      break;
    case PROP_RTP_AWARE:
      g_value_set_boolean (value, self->rtp_aware);
      break;
    case PROP_GSO_SUPPORTED:
      g_value_set_boolean (value, g_atomic_int_get (&self->gso_supported));
      break;
    case PROP_TXTIME_SUPPORTED:
      g_value_set_boolean (value, g_atomic_int_get (&self->txtime_supported));
      break;
    case PROP_QUEUED_PACKETS:
      g_mutex_lock (&self->queue_lock);
      g_value_set_uint (value, self->packet_queue.length);
      g_mutex_unlock (&self->queue_lock);
      break;
    case PROP_PACKETS_SENT:
      g_value_set_uint64 (value, stats.packets_sent);
      break;
    case PROP_BYTES_SENT:
      g_value_set_uint64 (value, stats.bytes_sent);
      break;
    case PROP_SYSTEM_CALLS:
      g_value_set_uint64 (value, stats.system_calls);
      break;
    case PROP_SENDMMSG_BATCHES:
      g_value_set_uint64 (value, stats.sendmmsg_batches);
      break;
    case PROP_GSO_BATCHES:
      g_value_set_uint64 (value, stats.gso_batches);
      break;
    case PROP_GSO_SEGMENTS:
      g_value_set_uint64 (value, stats.gso_segments);
      break;
    case PROP_FALLBACK_COUNT:
      g_value_set_uint64 (value, stats.fallback_count);
      break;
    case PROP_DROPPED_PACKETS:
      g_value_set_uint64 (value, stats.dropped_packets);
      break;
    case PROP_LATE_PACKETS:
      g_value_set_uint64 (value, stats.late_packets);
      break;
    case PROP_QUEUE_HIGH_WATERMARK:
      g_value_set_uint64 (value, stats.queue_high_watermark);
      break;
    case PROP_PACING_FALLBACKS:
      g_value_set_uint64 (value, stats.pacing_fallbacks);
      break;
    case PROP_RTP_PACKETS:
      g_value_set_uint64 (value, stats.rtp_packets);
      break;
    case PROP_RTP_INVALID_PACKETS:
      g_value_set_uint64 (value, stats.rtp_invalid_packets);
      break;
    case PROP_RTP_SEQUENCE_GAPS:
      g_value_set_uint64 (value, stats.rtp_sequence_gaps);
      break;
    case PROP_RTP_FRAME_BOUNDARIES:
      g_value_set_uint64 (value, stats.rtp_frame_boundaries);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
  }
}

static void
gst_udp_gso_sink_finalize (GObject *object)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (object);

  g_assert (self->worker == NULL);
  gst_udp_gso_sink_clear_queue_locked (self, FALSE);
  g_clear_pointer (&self->host, g_free);
  g_cond_clear (&self->drain_cond);
  g_cond_clear (&self->queue_cond);
  g_mutex_clear (&self->queue_lock);
  g_mutex_clear (&self->stats_lock);
  G_OBJECT_CLASS (gst_udp_gso_sink_parent_class)->finalize (object);
}

static void
gst_udp_gso_sink_class_init (GstUdpGsoSinkClass *klass)
{
  GObjectClass *object_class = G_OBJECT_CLASS (klass);
  GstElementClass *element_class = GST_ELEMENT_CLASS (klass);
  GstBaseSinkClass *base_sink_class = GST_BASE_SINK_CLASS (klass);
  GParamFlags ready_flags = G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS |
      GST_PARAM_MUTABLE_READY;

  object_class->set_property = gst_udp_gso_sink_set_property;
  object_class->get_property = gst_udp_gso_sink_get_property;
  object_class->finalize = gst_udp_gso_sink_finalize;

  g_object_class_install_property (object_class, PROP_HOST,
      g_param_spec_string ("host", "Host", "Destination hostname or IP address",
          DEFAULT_HOST, ready_flags));
  g_object_class_install_property (object_class, PROP_PORT,
      g_param_spec_uint ("port", "Port", "Destination UDP port", 1, 65535,
          DEFAULT_PORT, ready_flags));
  g_object_class_install_property (object_class, PROP_IO_MODE,
      g_param_spec_enum ("io-mode", "I/O mode", "UDP transmission backend",
          GST_TYPE_UDP_GSO_SINK_IO_MODE, GST_UDP_GSO_SINK_IO_AUTO, ready_flags));
  g_object_class_install_property (object_class, PROP_BATCH_SIZE,
      g_param_spec_uint ("batch-size", "Batch size",
          "Maximum datagrams submitted by one sendmmsg call", 1, 1024,
          DEFAULT_BATCH_SIZE, ready_flags));
  g_object_class_install_property (object_class, PROP_GSO_MIN_SEGMENTS,
      g_param_spec_uint ("gso-min-segments", "Minimum GSO segments",
          "Minimum equal-sized datagrams required for UDP GSO", 2,
          MAX_GSO_SEGMENTS, DEFAULT_GSO_MIN_SEGMENTS, ready_flags));
  g_object_class_install_property (object_class, PROP_GSO_MAX_SEGMENTS,
      g_param_spec_uint ("gso-max-segments", "Maximum GSO segments",
          "Maximum datagrams in one UDP GSO superpacket", 2,
          MAX_GSO_SEGMENTS, DEFAULT_GSO_MAX_SEGMENTS, ready_flags));
  g_object_class_install_property (object_class, PROP_FALLBACK,
      g_param_spec_boolean ("fallback", "Fallback",
          "Enable sendmmsg fallback for GSO and userspace fallback for TXTIME",
          TRUE,
          ready_flags));
  g_object_class_install_property (object_class, PROP_SEND_BUFFER_SIZE,
      g_param_spec_int ("send-buffer-size", "Send buffer size",
          "Requested SO_SNDBUF size in bytes; zero keeps the system default",
          0, G_MAXINT, 0, ready_flags));
  g_object_class_install_property (object_class, PROP_AGGREGATION_MODE,
      g_param_spec_enum ("aggregation-mode", "Aggregation mode",
          "Whether individual input buffers are accumulated for batched I/O",
          GST_TYPE_UDP_GSO_SINK_AGGREGATION_MODE,
          GST_UDP_GSO_SINK_AGGREGATION_BOUNDED, ready_flags));
  g_object_class_install_property (object_class, PROP_MAX_QUEUE_PACKETS,
      g_param_spec_uint ("max-queue-packets", "Maximum queued packets",
          "Maximum number of datagrams waiting in the sender queue", 1,
          65536, DEFAULT_MAX_QUEUE_PACKETS, ready_flags));
  g_object_class_install_property (object_class, PROP_MAX_BATCH_DELAY_US,
      g_param_spec_uint ("max-batch-delay-us", "Maximum batch delay",
          "Maximum time an input datagram waits for a larger batch", 0,
          G_USEC_PER_SEC, DEFAULT_MAX_BATCH_DELAY_US, ready_flags));
  g_object_class_install_property (object_class, PROP_BACKPRESSURE,
      g_param_spec_enum ("backpressure", "Backpressure policy",
          "Action taken when the bounded sender queue is full",
          GST_TYPE_UDP_GSO_SINK_BACKPRESSURE,
          GST_UDP_GSO_SINK_BACKPRESSURE_BLOCK, ready_flags));
  g_object_class_install_property (object_class, PROP_PACING_MODE,
      g_param_spec_enum ("pacing-mode", "Pacing mode",
          "Transmission-time backend used to pace UDP datagrams",
          GST_TYPE_UDP_GSO_SINK_PACING_MODE,
          GST_UDP_GSO_SINK_PACING_NONE, ready_flags));
  g_object_class_install_property (object_class, PROP_PACING_RATE,
      g_param_spec_uint64 ("pacing-rate", "Pacing rate",
          "Target wire rate in bits per second", 0, G_MAXUINT64,
          DEFAULT_PACING_RATE, ready_flags));
  g_object_class_install_property (object_class, PROP_PACING_LEAD_TIME_US,
      g_param_spec_uint ("pacing-lead-time-us", "Pacing lead time",
          "Minimum time between scheduling and transmission", 0,
          G_USEC_PER_SEC, DEFAULT_PACING_LEAD_TIME_US, ready_flags));
  g_object_class_install_property (object_class, PROP_MAX_PACING_HORIZON_US,
      g_param_spec_uint ("max-pacing-horizon-us", "Maximum pacing horizon",
          "Maximum future SO_TXTIME queued before applying backpressure; zero "
          "is unlimited",
          0, 10 * G_USEC_PER_SEC, DEFAULT_MAX_PACING_HORIZON_US, ready_flags));
  g_object_class_install_property (object_class, PROP_WIRE_OVERHEAD_BYTES,
      g_param_spec_uint ("wire-overhead-bytes", "Wire overhead",
          "Per-datagram bytes included in pacing calculations", 0, 512,
          DEFAULT_WIRE_OVERHEAD_BYTES, ready_flags));
  g_object_class_install_property (object_class, PROP_LATE_PACKET_POLICY,
      g_param_spec_enum ("late-packet-policy", "Late packet policy",
          "Whether a late datagram is rescheduled or dropped",
          GST_TYPE_UDP_GSO_SINK_LATE_POLICY, GST_UDP_GSO_SINK_LATE_SEND,
          ready_flags));
  g_object_class_install_property (object_class, PROP_RTP_AWARE,
      g_param_spec_boolean ("rtp-aware", "RTP aware",
          "Keep validated RTP frames in separate batches and collect RTP stats",
          FALSE, ready_flags));

  g_object_class_install_property (object_class, PROP_GSO_SUPPORTED,
      g_param_spec_boolean ("gso-supported", "GSO supported",
          "Whether UDP_SEGMENT is available on the active socket", FALSE,
          G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_TXTIME_SUPPORTED,
      g_param_spec_boolean ("txtime-supported", "SO_TXTIME supported",
          "Whether SO_TXTIME was accepted on the active socket", FALSE,
          G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_QUEUED_PACKETS,
      g_param_spec_uint ("queued-packets", "Queued packets",
          "Current number of datagrams waiting in the sender queue", 0,
          65536, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_PACKETS_SENT,
      g_param_spec_uint64 ("packets-sent", "Packets sent",
          "Number of successfully transmitted UDP datagrams", 0, G_MAXUINT64,
          0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_BYTES_SENT,
      g_param_spec_uint64 ("bytes-sent", "Bytes sent",
          "Number of successfully transmitted UDP payload bytes", 0,
          G_MAXUINT64, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_SYSTEM_CALLS,
      g_param_spec_uint64 ("system-calls", "System calls",
          "Number of sendmsg or sendmmsg attempts", 0, G_MAXUINT64, 0,
          G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_SENDMMSG_BATCHES,
      g_param_spec_uint64 ("sendmmsg-batches", "sendmmsg batches",
          "Number of sendmmsg submission attempts", 0, G_MAXUINT64, 0,
          G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_GSO_BATCHES,
      g_param_spec_uint64 ("gso-batches", "GSO batches",
          "Number of successfully transmitted UDP GSO superpackets", 0,
          G_MAXUINT64, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_GSO_SEGMENTS,
      g_param_spec_uint64 ("gso-segments", "GSO segments",
          "Number of UDP datagrams transmitted through UDP GSO", 0,
          G_MAXUINT64, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_FALLBACK_COUNT,
      g_param_spec_uint64 ("fallback-count", "Fallback count",
          "Number of failed GSO operations retried through sendmmsg", 0,
          G_MAXUINT64, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_DROPPED_PACKETS,
      g_param_spec_uint64 ("dropped-packets", "Dropped packets",
          "Number of datagrams dropped by queue or late-packet policy", 0,
          G_MAXUINT64, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_LATE_PACKETS,
      g_param_spec_uint64 ("late-packets", "Late packets",
          "Number of datagrams processed after their intended send time", 0,
          G_MAXUINT64, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_QUEUE_HIGH_WATERMARK,
      g_param_spec_uint64 ("queue-high-watermark", "Queue high watermark",
          "Largest observed number of waiting datagrams", 0, G_MAXUINT64, 0,
          G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_PACING_FALLBACKS,
      g_param_spec_uint64 ("pacing-fallbacks", "Pacing fallbacks",
          "Number of transitions from SO_TXTIME to userspace pacing", 0,
          G_MAXUINT64, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_RTP_PACKETS,
      g_param_spec_uint64 ("rtp-packets", "RTP packets",
          "Number of input datagrams with a validated RTP fixed header", 0,
          G_MAXUINT64, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_RTP_INVALID_PACKETS,
      g_param_spec_uint64 ("rtp-invalid-packets", "Invalid RTP packets",
          "Number of datagrams that failed basic RTP header validation", 0,
          G_MAXUINT64, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_RTP_SEQUENCE_GAPS,
      g_param_spec_uint64 ("rtp-sequence-gaps", "RTP sequence gaps",
          "Number of discontinuities in the observed RTP sequence numbers", 0,
          G_MAXUINT64, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (object_class, PROP_RTP_FRAME_BOUNDARIES,
      g_param_spec_uint64 ("rtp-frame-boundaries", "RTP frame boundaries",
          "Number of RTP frame boundaries observed while batching", 0,
          G_MAXUINT64, 0, G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));

  gst_element_class_set_static_metadata (element_class,
      "UDP GSO sink", "Sink/Network",
      "Batches and paces UDP datagrams using sendmmsg, UDP GSO, or SO_TXTIME",
      "gst-plugin-udp-gso contributors");
  gst_element_class_add_static_pad_template (element_class, &sink_template);

  base_sink_class->start = GST_DEBUG_FUNCPTR (gst_udp_gso_sink_start);
  base_sink_class->stop = GST_DEBUG_FUNCPTR (gst_udp_gso_sink_stop);
  base_sink_class->unlock = GST_DEBUG_FUNCPTR (gst_udp_gso_sink_unlock);
  base_sink_class->unlock_stop =
      GST_DEBUG_FUNCPTR (gst_udp_gso_sink_unlock_stop);
  base_sink_class->event = GST_DEBUG_FUNCPTR (gst_udp_gso_sink_event);
  base_sink_class->render = GST_DEBUG_FUNCPTR (gst_udp_gso_sink_render);
  base_sink_class->render_list =
      GST_DEBUG_FUNCPTR (gst_udp_gso_sink_render_list);

  GST_DEBUG_CATEGORY_INIT (gst_udp_gso_sink_debug, "udpgsosink", 0,
      "Linux UDP GSO sink");
}

static void
gst_udp_gso_sink_init (GstUdpGsoSink *self)
{
  long configured_iov_max;

  self->host = g_strdup (DEFAULT_HOST);
  self->port = DEFAULT_PORT;
  self->io_mode = GST_UDP_GSO_SINK_IO_AUTO;
  self->batch_size = DEFAULT_BATCH_SIZE;
  self->gso_min_segments = DEFAULT_GSO_MIN_SEGMENTS;
  self->gso_max_segments = DEFAULT_GSO_MAX_SEGMENTS;
  self->fallback = TRUE;
  self->send_buffer_size = 0;
  self->aggregation_mode = GST_UDP_GSO_SINK_AGGREGATION_BOUNDED;
  self->max_queue_packets = DEFAULT_MAX_QUEUE_PACKETS;
  self->max_batch_delay_us = DEFAULT_MAX_BATCH_DELAY_US;
  self->backpressure = GST_UDP_GSO_SINK_BACKPRESSURE_BLOCK;
  self->pacing_mode = GST_UDP_GSO_SINK_PACING_NONE;
  self->pacing_rate = DEFAULT_PACING_RATE;
  self->pacing_lead_time_us = DEFAULT_PACING_LEAD_TIME_US;
  self->max_pacing_horizon_us = DEFAULT_MAX_PACING_HORIZON_US;
  self->wire_overhead_bytes = DEFAULT_WIRE_OVERHEAD_BYTES;
  self->late_packet_policy = GST_UDP_GSO_SINK_LATE_SEND;
  self->rtp_aware = FALSE;

  self->fd = -1;
  g_atomic_int_set (&self->gso_supported, FALSE);
  g_atomic_int_set (&self->txtime_supported, FALSE);
  self->txtime_active = FALSE;
  self->gso_warning_emitted = FALSE;
  self->txtime_warning_emitted = FALSE;

  configured_iov_max = sysconf (_SC_IOV_MAX);
  self->iov_max = configured_iov_max > 0 ? configured_iov_max : 1024;

  g_mutex_init (&self->queue_lock);
  g_cond_init (&self->queue_cond);
  g_cond_init (&self->drain_cond);
  g_queue_init (&self->packet_queue);
  self->worker = NULL;
  self->worker_stop = FALSE;
  self->worker_flush = FALSE;
  self->worker_busy = FALSE;
  self->unlocked = FALSE;
  self->worker_flow = GST_FLOW_OK;
  memset (&self->schedule, 0, sizeof (self->schedule));
  self->last_rtp_valid = FALSE;

  g_mutex_init (&self->stats_lock);
  memset (&self->stats, 0, sizeof (self->stats));
}
