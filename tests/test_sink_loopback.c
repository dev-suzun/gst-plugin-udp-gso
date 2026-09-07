/* SPDX-License-Identifier: BSD-3-Clause */

#include "gstudpgsosink.h"

#include <arpa/inet.h>
#include <gst/app/gstappsrc.h>
#include <gst/gst.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define PACKET_COUNT 8
#define PACKET_SIZE 1200

static GstBuffer *
make_rtp_packet (guint16 sequence, gboolean marker)
{
  GstBuffer *buffer = gst_buffer_new_allocate (NULL, PACKET_SIZE, NULL);
  GstMapInfo map;

  g_assert_true (gst_buffer_map (buffer, &map, GST_MAP_WRITE));
  memset (map.data, sequence & 0xff, map.size);
  map.data[0] = 0x80;
  map.data[1] = (marker ? 0x80 : 0) | 96;
  map.data[2] = sequence >> 8;
  map.data[3] = sequence & 0xff;
  map.data[4] = 0x00;
  map.data[5] = 0x01;
  map.data[6] = 0x5f;
  map.data[7] = 0x90;
  map.data[8] = 0x12;
  map.data[9] = 0x34;
  map.data[10] = 0x56;
  map.data[11] = 0x78;
  gst_buffer_unmap (buffer, &map);
  GST_BUFFER_PTS (buffer) = sequence * GST_MSECOND;
  return buffer;
}

static void
test_individual_buffers_are_batched (void)
{
  struct sockaddr_in address = { 0 };
  socklen_t address_length = sizeof (address);
  struct timeval timeout = { .tv_sec = 2, .tv_usec = 0 };
  guint8 received[PACKET_SIZE + 1];
  GstElement *pipeline;
  GstElement *source;
  GstElement *sink;
  GstBus *bus;
  GstCaps *caps;
  GstMessage *message;
  guint64 packets_sent = 0;
  guint64 system_calls = 0;
  guint64 gso_batches = 0;
  guint64 sendmmsg_batches = 0;
  guint64 rtp_packets = 0;
  guint64 sequence_gaps = 0;
  guint port;
  gint receiver;
  guint i;

  receiver = socket (AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  g_assert_cmpint (receiver, >=, 0);
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl (INADDR_LOOPBACK);
  address.sin_port = 0;
  g_assert_cmpint (bind (receiver, (struct sockaddr *) &address,
      sizeof (address)), ==, 0);
  g_assert_cmpint (getsockname (receiver, (struct sockaddr *) &address,
      &address_length), ==, 0);
  g_assert_cmpint (setsockopt (receiver, SOL_SOCKET, SO_RCVTIMEO, &timeout,
      sizeof (timeout)), ==, 0);
  port = ntohs (address.sin_port);

  pipeline = gst_pipeline_new ("udpgsosink-loopback-test");
  source = gst_element_factory_make ("appsrc", "source");
  sink = g_object_new (GST_TYPE_UDP_GSO_SINK,
      "host", "127.0.0.1",
      "port", port,
      "batch-size", PACKET_COUNT,
      "max-batch-delay-us", 50000,
      "rtp-aware", TRUE,
      "sync", FALSE,
      "async", FALSE,
      NULL);
  g_assert_nonnull (pipeline);
  g_assert_nonnull (source);
  g_assert_nonnull (sink);
  gst_util_set_object_arg (G_OBJECT (sink), "io-mode", "gso");
  gst_util_set_object_arg (G_OBJECT (sink), "aggregation-mode", "bounded");
  caps = gst_caps_new_simple ("application/x-rtp",
      "media", G_TYPE_STRING, "video",
      "encoding-name", G_TYPE_STRING, "H264",
      "clock-rate", G_TYPE_INT, 90000,
      "payload", G_TYPE_INT, 96, NULL);
  g_object_set (source, "caps", caps, "format", GST_FORMAT_TIME, NULL);
  gst_caps_unref (caps);
  gst_bin_add_many (GST_BIN (pipeline), source, sink, NULL);
  g_assert_true (gst_element_link (source, sink));
  g_assert_cmpint (gst_element_set_state (pipeline, GST_STATE_PLAYING), !=,
      GST_STATE_CHANGE_FAILURE);

  for (i = 0; i < PACKET_COUNT; i++) {
    GstFlowReturn flow = gst_app_src_push_buffer (GST_APP_SRC (source),
        make_rtp_packet ((guint16) (100 + i), i == PACKET_COUNT - 1));
    g_assert_cmpint (flow, ==, GST_FLOW_OK);
  }
  g_assert_cmpint (gst_app_src_end_of_stream (GST_APP_SRC (source)), ==,
      GST_FLOW_OK);
  bus = gst_element_get_bus (pipeline);
  message = gst_bus_timed_pop_filtered (bus, 5 * GST_SECOND,
      GST_MESSAGE_EOS | GST_MESSAGE_ERROR);
  g_assert_nonnull (message);
  g_assert_cmpint (GST_MESSAGE_TYPE (message), ==, GST_MESSAGE_EOS);
  gst_message_unref (message);
  gst_object_unref (bus);

  for (i = 0; i < PACKET_COUNT; i++) {
    ssize_t length = recv (receiver, received, sizeof (received), 0);

    g_assert_cmpint (length, ==, PACKET_SIZE);
    g_assert_cmpuint ((((guint16) received[2]) << 8) | received[3], ==,
        100 + i);
  }

  g_object_get (sink,
      "packets-sent", &packets_sent,
      "system-calls", &system_calls,
      "gso-batches", &gso_batches,
      "sendmmsg-batches", &sendmmsg_batches,
      "rtp-packets", &rtp_packets,
      "rtp-sequence-gaps", &sequence_gaps,
      NULL);
  g_assert_cmpuint (packets_sent, ==, PACKET_COUNT);
  g_assert_cmpuint (system_calls, <, PACKET_COUNT);
  g_assert_cmpuint (gso_batches + sendmmsg_batches, >, 0);
  g_assert_cmpuint (rtp_packets, ==, PACKET_COUNT);
  g_assert_cmpuint (sequence_gaps, ==, 0);

  gst_element_set_state (pipeline, GST_STATE_NULL);
  gst_object_unref (pipeline);
  close (receiver);
}

int
main (int argc, char **argv)
{
  gst_init (&argc, &argv);
  g_test_init (&argc, &argv, NULL);
  g_test_add_func ("/udpgsosink/individual-buffers-are-batched",
      test_individual_buffers_are_batched);
  return g_test_run ();
}

