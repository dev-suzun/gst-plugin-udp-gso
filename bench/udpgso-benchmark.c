/* SPDX-License-Identifier: BSD-3-Clause */

#include <gst/app/gstappsrc.h>
#include <gst/gst.h>

static gboolean
wait_for_eos (GstElement *pipeline)
{
  GstBus *bus = gst_element_get_bus (pipeline);
  GstMessage *message = gst_bus_timed_pop_filtered (bus, GST_CLOCK_TIME_NONE,
      GST_MESSAGE_EOS | GST_MESSAGE_ERROR);
  gboolean success = FALSE;

  if (message != NULL && GST_MESSAGE_TYPE (message) == GST_MESSAGE_EOS) {
    success = TRUE;
  } else if (message != NULL) {
    GError *error = NULL;
    gchar *debug = NULL;

    gst_message_parse_error (message, &error, &debug);
    g_printerr ("Pipeline error: %s\n", error->message);
    if (debug != NULL)
      g_printerr ("Debug details: %s\n", debug);
    g_clear_error (&error);
    g_free (debug);
  }

  if (message != NULL)
    gst_message_unref (message);
  gst_object_unref (bus);
  return success;
}

int
main (int argc, char **argv)
{
  gchar *sink_name = g_strdup ("udpgsosink");
  gchar *host = g_strdup ("127.0.0.1");
  gchar *io_mode = g_strdup ("auto");
  gchar *aggregation_mode = g_strdup ("bounded");
  gchar *pacing_mode = g_strdup ("none");
  gint port = 5004;
  gint packet_count = 100000;
  gint packet_size = 1200;
  gint batch_size = 32;
  gint batch_delay_us = 1000;
  gint64 pacing_rate = 50000000;
  gint pacing_lead_us = 2000;
  gint pacing_horizon_us = 100000;
  GOptionEntry options[] = {
    { "sink", 0, 0, G_OPTION_ARG_STRING, &sink_name,
      "Sink factory: udpgsosink or udpsink", "NAME" },
    { "host", 0, 0, G_OPTION_ARG_STRING, &host,
      "Receiver address", "ADDRESS" },
    { "port", 0, 0, G_OPTION_ARG_INT, &port,
      "Receiver UDP port", "PORT" },
    { "packets", 0, 0, G_OPTION_ARG_INT, &packet_count,
      "Number of datagrams", "COUNT" },
    { "packet-size", 0, 0, G_OPTION_ARG_INT, &packet_size,
      "Payload bytes per datagram", "BYTES" },
    { "io-mode", 0, 0, G_OPTION_ARG_STRING, &io_mode,
      "udpgsosink I/O mode", "MODE" },
    { "aggregation-mode", 0, 0, G_OPTION_ARG_STRING, &aggregation_mode,
      "udpgsosink aggregation mode", "MODE" },
    { "batch-size", 0, 0, G_OPTION_ARG_INT, &batch_size,
      "udpgsosink batch size", "COUNT" },
    { "batch-delay-us", 0, 0, G_OPTION_ARG_INT, &batch_delay_us,
      "udpgsosink maximum batch delay", "MICROSECONDS" },
    { "pacing-mode", 0, 0, G_OPTION_ARG_STRING, &pacing_mode,
      "udpgsosink pacing mode", "MODE" },
    { "pacing-rate", 0, 0, G_OPTION_ARG_INT64, &pacing_rate,
      "udpgsosink target pacing rate", "BITS_PER_SECOND" },
    { "pacing-lead-us", 0, 0, G_OPTION_ARG_INT, &pacing_lead_us,
      "udpgsosink pacing lead time", "MICROSECONDS" },
    { "pacing-horizon-us", 0, 0, G_OPTION_ARG_INT, &pacing_horizon_us,
      "udpgsosink maximum future kernel schedule", "MICROSECONDS" },
    { NULL }
  };
  GOptionContext *context;
  GError *error = NULL;
  GstElement *pipeline;
  GstElement *source;
  GstElement *sink;
  gint64 started_us;
  gint64 elapsed_us;
  guint64 total_bytes;
  guint64 packets_sent = 0;
  guint64 calls = 0;
  guint64 gso_batches = 0;
  guint64 sendmmsg_batches = 0;
  guint64 dropped_packets = 0;
  guint64 late_packets = 0;
  guint64 queue_high_watermark = 0;
  gboolean success = FALSE;
  gint i;

  context = g_option_context_new ("- benchmark a UDP GStreamer sink");
  g_option_context_add_main_entries (context, options, NULL);
  g_option_context_add_group (context, gst_init_get_option_group ());
  if (!g_option_context_parse (context, &argc, &argv, &error)) {
    g_printerr ("Option error: %s\n", error->message);
    g_clear_error (&error);
    goto out_options;
  }
  if (port < 1 || port > 65535 || packet_count < 1 || packet_size < 1 ||
      packet_size > 65507 || batch_size < 1 || batch_size > 1024 ||
      batch_delay_us < 0 || batch_delay_us > G_USEC_PER_SEC ||
      pacing_rate < 1 || pacing_lead_us < 0 ||
      pacing_lead_us > G_USEC_PER_SEC || pacing_horizon_us < 0 ||
      pacing_horizon_us > 10 * G_USEC_PER_SEC) {
    g_printerr ("One or more numeric options are outside their valid range.\n");
    goto out_options;
  }

  pipeline = gst_pipeline_new ("udpgso-benchmark");
  source = gst_element_factory_make ("appsrc", "source");
  sink = gst_element_factory_make (sink_name, "sink");
  if (pipeline == NULL || source == NULL || sink == NULL) {
    g_printerr ("Could not create pipeline elements; check --sink and "
        "GST_PLUGIN_PATH.\n");
    if (pipeline != NULL)
      gst_object_unref (pipeline);
    if (source != NULL)
      gst_object_unref (source);
    if (sink != NULL)
      gst_object_unref (sink);
    goto out_options;
  }

  g_object_set (source,
      "format", GST_FORMAT_BYTES,
      "block", TRUE,
      "max-bytes", (guint64) packet_size * 1024,
      NULL);
  g_object_set (sink,
      "host", host,
      "port", port,
      "sync", FALSE,
      "async", FALSE,
      NULL);
  if (g_str_equal (sink_name, "udpgsosink")) {
    g_object_set (sink,
        "batch-size", batch_size,
        "max-batch-delay-us", batch_delay_us,
        "pacing-rate", (guint64) pacing_rate,
        "pacing-lead-time-us", pacing_lead_us,
        "max-pacing-horizon-us", pacing_horizon_us,
        NULL);
    gst_util_set_object_arg (G_OBJECT (sink), "io-mode", io_mode);
    gst_util_set_object_arg (G_OBJECT (sink), "aggregation-mode",
        aggregation_mode);
    gst_util_set_object_arg (G_OBJECT (sink), "pacing-mode", pacing_mode);
  }

  gst_bin_add_many (GST_BIN (pipeline), source, sink, NULL);
  if (!gst_element_link (source, sink)) {
    g_printerr ("Could not link appsrc to %s.\n", sink_name);
    goto out_pipeline;
  }
  if (gst_element_set_state (pipeline, GST_STATE_PLAYING) ==
      GST_STATE_CHANGE_FAILURE) {
    g_printerr ("Could not start the benchmark pipeline.\n");
    goto out_pipeline;
  }

  started_us = g_get_monotonic_time ();
  for (i = 0; i < packet_count; i++) {
    GstBuffer *buffer = gst_buffer_new_allocate (NULL, packet_size, NULL);

    gst_buffer_memset (buffer, 0, i & 0xff, packet_size);
    if (gst_app_src_push_buffer (GST_APP_SRC (source), buffer) != GST_FLOW_OK) {
      g_printerr ("appsrc stopped while pushing packet %d.\n", i);
      goto out_pipeline;
    }
  }
  if (gst_app_src_end_of_stream (GST_APP_SRC (source)) != GST_FLOW_OK) {
    g_printerr ("Could not submit EOS to appsrc.\n");
    goto out_pipeline;
  }
  if (!wait_for_eos (pipeline))
    goto out_pipeline;
  elapsed_us = g_get_monotonic_time () - started_us;
  total_bytes = ((guint64) packet_count) * packet_size;

  g_print ("sink=%s packets=%d bytes=%" G_GUINT64_FORMAT
      " elapsed=%.6f s rate=%.2f Mbit/s packets/s=%.0f\n",
      sink_name, packet_count, total_bytes, elapsed_us / 1000000.0,
      elapsed_us > 0 ? total_bytes * 8.0 / elapsed_us : 0.0,
      elapsed_us > 0 ? packet_count * 1000000.0 / elapsed_us : 0.0);
  if (g_str_equal (sink_name, "udpgsosink")) {
    g_object_get (sink,
        "packets-sent", &packets_sent,
        "system-calls", &calls,
        "gso-batches", &gso_batches,
        "sendmmsg-batches", &sendmmsg_batches,
        "dropped-packets", &dropped_packets,
        "late-packets", &late_packets,
        "queue-high-watermark", &queue_high_watermark,
        NULL);
    g_print ("plugin packets=%" G_GUINT64_FORMAT
        " calls=%" G_GUINT64_FORMAT
        " packets/call=%.2f gso-batches=%" G_GUINT64_FORMAT
        " sendmmsg-batches=%" G_GUINT64_FORMAT
        " queue-high=%" G_GUINT64_FORMAT
        " dropped=%" G_GUINT64_FORMAT " late=%" G_GUINT64_FORMAT "\n",
        packets_sent, calls, calls > 0 ? (gdouble) packets_sent / calls : 0.0,
        gso_batches, sendmmsg_batches, queue_high_watermark, dropped_packets,
        late_packets);
  }
  success = TRUE;

out_pipeline:
  gst_element_set_state (pipeline, GST_STATE_NULL);
  gst_object_unref (pipeline);
out_options:
  g_option_context_free (context);
  g_free (sink_name);
  g_free (host);
  g_free (io_mode);
  g_free (aggregation_mode);
  g_free (pacing_mode);
  return success ? 0 : 1;
}

