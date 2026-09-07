/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "gstudpgsosink.h"

#include "udpgsobatch.h"

#include <errno.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#ifndef UDP_SEGMENT
#define UDP_SEGMENT 103
#endif

#ifndef SOL_UDP
#define SOL_UDP IPPROTO_UDP
#endif

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 5004
#define DEFAULT_BATCH_SIZE 32
#define DEFAULT_GSO_MIN_SEGMENTS 4
#define DEFAULT_GSO_MAX_SEGMENTS 32
#define MAX_GSO_SEGMENTS 64

GST_DEBUG_CATEGORY_STATIC (gst_udp_gso_sink_debug);
#define GST_CAT_DEFAULT gst_udp_gso_sink_debug

typedef enum
{
  GST_UDP_GSO_SINK_IO_AUTO,
  GST_UDP_GSO_SINK_IO_SENDMSG,
  GST_UDP_GSO_SINK_IO_SENDMMSG,
  GST_UDP_GSO_SINK_IO_GSO,
} GstUdpGsoSinkIoMode;

typedef struct
{
  GstMapInfo *maps;
  struct iovec *iov;
  guint n_iov;
  gsize size;
} GstUdpGsoSinkMappedPacket;

typedef struct
{
  guint64 packets_sent;
  guint64 bytes_sent;
  guint64 system_calls;
  guint64 sendmmsg_batches;
  guint64 gso_batches;
  guint64 gso_segments;
  guint64 fallback_count;
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

  gint fd;
  gint gso_supported;
  gboolean gso_warning_emitted;
  long iov_max;

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
  PROP_GSO_SUPPORTED,
  PROP_PACKETS_SENT,
  PROP_BYTES_SENT,
  PROP_SYSTEM_CALLS,
  PROP_SENDMMSG_BATCHES,
  PROP_GSO_BATCHES,
  PROP_GSO_SEGMENTS,
  PROP_FALLBACK_COUNT,
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

static GstFlowReturn
gst_udp_gso_sink_send_one (GstUdpGsoSink *self,
                        GstUdpGsoSinkMappedPacket *packet)
{
  struct msghdr message = { 0 };
  ssize_t sent;
  guint64 attempts = 0;

  message.msg_iov = packet->iov;
  message.msg_iovlen = packet->n_iov;

  sent = gst_udp_gso_sink_sendmsg_retry (self, &message, &attempts);
  if (sent < 0) {
    gst_udp_gso_sink_stats_add (self, 0, 0, attempts, 0, 0, 0, 0);
    return gst_udp_gso_sink_report_send_error (self, "sendmsg", errno);
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
                          guint count)
{
  struct mmsghdr *messages;
  guint completed = 0;
  guint i;

  if (count == 0)
    return GST_FLOW_OK;
  if (count == 1)
    return gst_udp_gso_sink_send_one (self, packets);

  messages = g_new0 (struct mmsghdr, count);
  for (i = 0; i < count; i++) {
    messages[i].msg_hdr.msg_iov = packets[i].iov;
    messages[i].msg_hdr.msg_iovlen = packets[i].n_iov;
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
      gst_udp_gso_sink_stats_add (self, 0, 0, attempts, attempts, 0, 0, 0);
      g_free (messages);
      return gst_udp_gso_sink_report_send_error (self, "sendmmsg", saved_errno);
    }

    if (result == 0) {
      gst_udp_gso_sink_stats_add (self, 0, 0, attempts, attempts, 0, 0, 0);
      g_free (messages);
      return gst_udp_gso_sink_report_send_error (self, "zero-length sendmmsg result",
          EIO);
    }

    for (i = 0; i < (guint) result; i++) {
      guint index = completed + i;

      if (messages[index].msg_len != packets[index].size) {
        gst_udp_gso_sink_stats_add (self, i, batch_bytes, attempts, attempts,
            0, 0, 0);
        g_free (messages);
        return gst_udp_gso_sink_report_send_error (self, "short sendmmsg", EIO);
      }
      batch_bytes += packets[index].size;
    }

    gst_udp_gso_sink_stats_add (self, result, batch_bytes, attempts, attempts,
        0, 0, 0);
    completed += result;
  }

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
      return gst_udp_gso_sink_send_batch (self, packets, count);
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
      GstFlowReturn flow = gst_udp_gso_sink_send_one (self, &packets[offset++]);
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
          &packets[offset], batch);
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

        flow = gst_udp_gso_sink_send_batch (self, &packets[offset], batch);
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

    flow = gst_udp_gso_sink_send_batch (self, &packets[offset], run);
    if (flow != GST_FLOW_OK) {
      g_free (sizes);
      return flow;
    }
    offset += run;
  }

  g_free (sizes);
  return GST_FLOW_OK;
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
  if (self->io_mode == GST_UDP_GSO_SINK_IO_GSO &&
      !g_atomic_int_get (&self->gso_supported) && !self->fallback) {
    GST_ELEMENT_ERROR (self, RESOURCE, SETTINGS,
        ("UDP GSO was requested but is unavailable"),
        ("Enable fallback or select another io-mode"));
    close (self->fd);
    self->fd = -1;
    return FALSE;
  }

  GST_INFO_OBJECT (self, "sending to %s:%u with io-mode=%d, GSO=%s",
      self->host, self->port, self->io_mode,
      g_atomic_int_get (&self->gso_supported) ? "available" : "unavailable");
  return TRUE;
}

static gboolean
gst_udp_gso_sink_stop (GstBaseSink *base_sink)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (base_sink);

  if (self->fd >= 0) {
    close (self->fd);
    self->fd = -1;
  }
  g_atomic_int_set (&self->gso_supported, FALSE);
  return TRUE;
}

static GstFlowReturn
gst_udp_gso_sink_render (GstBaseSink *base_sink, GstBuffer *buffer)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (base_sink);
  GstUdpGsoSinkMappedPacket packet = { 0 };
  GstFlowReturn flow;

  if (!gst_udp_gso_sink_map_packet (self, buffer, &packet))
    return GST_FLOW_ERROR;

  /* One GstBuffer is one UDP datagram; it must never be segmented implicitly. */
  flow = gst_udp_gso_sink_send_one (self, &packet);
  gst_udp_gso_sink_mapped_packet_clear (&packet);
  return flow;
}

static GstFlowReturn
gst_udp_gso_sink_render_list (GstBaseSink *base_sink, GstBufferList *buffer_list)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (base_sink);
  GstUdpGsoSinkMappedPacket *packets;
  GstBuffer **buffers;
  GstFlowReturn flow;
  guint count;
  guint i;

  count = gst_buffer_list_length (buffer_list);
  if (count == 0)
    return GST_FLOW_OK;

  buffers = g_new (GstBuffer *, count);
  packets = g_new0 (GstUdpGsoSinkMappedPacket, count);
  for (i = 0; i < count; i++)
    buffers[i] = gst_buffer_list_get (buffer_list, i);

  if (!gst_udp_gso_sink_map_buffers (self, buffers, count, packets)) {
    g_free (packets);
    g_free (buffers);
    return GST_FLOW_ERROR;
  }

  flow = gst_udp_gso_sink_send_with_mode (self, packets, count);
  gst_udp_gso_sink_unmap_packets (packets, count);
  g_free (packets);
  g_free (buffers);
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
    case PROP_GSO_SUPPORTED:
      g_value_set_boolean (value, g_atomic_int_get (&self->gso_supported));
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
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
  }
}

static void
gst_udp_gso_sink_finalize (GObject *object)
{
  GstUdpGsoSink *self = GST_UDP_GSO_SINK (object);

  g_clear_pointer (&self->host, g_free);
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
          "Fall back to sendmmsg when UDP GSO is unsupported", TRUE,
          ready_flags));
  g_object_class_install_property (object_class, PROP_SEND_BUFFER_SIZE,
      g_param_spec_int ("send-buffer-size", "Send buffer size",
          "Requested SO_SNDBUF size in bytes; zero keeps the system default",
          0, G_MAXINT, 0, ready_flags));

  g_object_class_install_property (object_class, PROP_GSO_SUPPORTED,
      g_param_spec_boolean ("gso-supported", "GSO supported",
          "Whether UDP_SEGMENT is available on the active socket", FALSE,
          G_PARAM_READABLE | G_PARAM_STATIC_STRINGS));
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

  gst_element_class_set_static_metadata (element_class,
      "UDP GSO sink", "Sink/Network",
      "Sends UDP datagrams using sendmsg, sendmmsg, or Linux UDP GSO",
      "gst-plugin-udp-gso contributors");
  gst_element_class_add_static_pad_template (element_class, &sink_template);

  base_sink_class->start = GST_DEBUG_FUNCPTR (gst_udp_gso_sink_start);
  base_sink_class->stop = GST_DEBUG_FUNCPTR (gst_udp_gso_sink_stop);
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

  self->fd = -1;
  g_atomic_int_set (&self->gso_supported, FALSE);
  self->gso_warning_emitted = FALSE;

  configured_iov_max = sysconf (_SC_IOV_MAX);
  self->iov_max = configured_iov_max > 0 ? configured_iov_max : 1024;

  g_mutex_init (&self->stats_lock);
  memset (&self->stats, 0, sizeof (self->stats));
}

