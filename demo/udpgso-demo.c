/* SPDX-License-Identifier: BSD-3-Clause */

#include <gst/gst.h>
#include <gtk/gtk.h>

typedef struct
{
  GtkWidget *window;
  GtkWidget *input_combo;
  GtkWidget *input_stack;
  GtkWidget *file_chooser;
  GtkWidget *camera_device_entry;
  GtkWidget *camera_resolution_combo;
  GtkWidget *camera_framerate_spin;
  GtkWidget *camera_bitrate_spin;
  GtkWidget *host_entry;
  GtkWidget *port_spin;
  GtkWidget *mode_combo;
  GtkWidget *batch_spin;
  GtkWidget *fallback_check;
  GtkWidget *start_button;
  GtkWidget *stop_button;
  GtkWidget *status_label;
  GtkWidget *codec_label;
  GtkWidget *progress;
  GtkWidget *packets_label;
  GtkWidget *bytes_label;
  GtkWidget *calls_label;
  GtkWidget *efficiency_label;
  GtkWidget *bitrate_label;
  GtkWidget *gso_support_label;
  GtkWidget *gso_batches_label;
  GtkWidget *fallbacks_label;

  GstElement *pipeline;
  GstElement *sink;
  GstElement *camera_queue;
  guint bus_watch_id;
  guint stats_timer_id;
  gint video_linked;
  guint64 previous_bytes;
  gint64 previous_sample_us;
} DemoApp;

typedef struct
{
  DemoApp *app;
  gchar *text;
} UiTextUpdate;

static void stop_pipeline (DemoApp * app, const gchar * final_status);

static void
set_label_uint64 (GtkWidget * label, guint64 value)
{
  gchar *text = g_strdup_printf ("%" G_GUINT64_FORMAT, value);

  gtk_label_set_text (GTK_LABEL (label), text);
  g_free (text);
}

static gchar *
format_clock_time (GstClockTime value)
{
  guint64 total_seconds;

  if (!GST_CLOCK_TIME_IS_VALID (value))
    return g_strdup ("--:--");

  total_seconds = value / GST_SECOND;
  return g_strdup_printf ("%02" G_GUINT64_FORMAT ":%02" G_GUINT64_FORMAT,
      total_seconds / 60, total_seconds % 60);
}

static void
set_controls_running (DemoApp * app, gboolean running)
{
  gtk_widget_set_sensitive (app->input_combo, !running);
  gtk_widget_set_sensitive (app->input_stack, !running);
  gtk_widget_set_sensitive (app->host_entry, !running);
  gtk_widget_set_sensitive (app->port_spin, !running);
  gtk_widget_set_sensitive (app->mode_combo, !running);
  gtk_widget_set_sensitive (app->batch_spin, !running);
  gtk_widget_set_sensitive (app->fallback_check, !running);
  gtk_widget_set_sensitive (app->start_button, !running);
  gtk_widget_set_sensitive (app->stop_button, running);
}

static gboolean
apply_codec_text (gpointer user_data)
{
  UiTextUpdate *update = user_data;

  gtk_label_set_text (GTK_LABEL (update->app->codec_label), update->text);
  g_free (update->text);
  g_free (update);
  return G_SOURCE_REMOVE;
}

static void
queue_codec_text (DemoApp * app, const gchar * text)
{
  UiTextUpdate *update = g_new0 (UiTextUpdate, 1);

  update->app = app;
  update->text = g_strdup (text);
  g_main_context_invoke (NULL, apply_codec_text, update);
}

static void
post_stream_error (DemoApp * app, const gchar * summary, const gchar * detail)
{
  GError *error = g_error_new_literal (GST_STREAM_ERROR,
      GST_STREAM_ERROR_FORMAT, summary);
  GstMessage *message = gst_message_new_error (GST_OBJECT (app->pipeline),
      error, detail);

  g_error_free (error);
  gst_element_post_message (app->pipeline, message);
}

static void
on_demux_pad_added (GstElement * demux, GstPad * pad, gpointer user_data)
{
  DemoApp *app = user_data;
  GstCaps *caps;
  const GstStructure *structure;
  const gchar *caps_name;
  const gchar *parser_name = NULL;
  const gchar *payloader_name = NULL;
  const gchar *codec_text = NULL;
  GstElement *queue;
  GstElement *parser;
  GstElement *payloader;
  GstPad *queue_sink_pad;
  GstPadLinkReturn pad_result;

  (void) demux;

  caps = gst_pad_get_current_caps (pad);
  if (caps == NULL)
    caps = gst_pad_query_caps (pad, NULL);
  if (caps == NULL || gst_caps_is_empty (caps)) {
    if (caps != NULL)
      gst_caps_unref (caps);
    return;
  }

  structure = gst_caps_get_structure (caps, 0);
  caps_name = gst_structure_get_name (structure);

  if (g_str_equal (caps_name, "video/x-h264")) {
    parser_name = "h264parse";
    payloader_name = "rtph264pay";
    codec_text = "H.264 over RTP (payload type 96)";
  } else if (g_str_equal (caps_name, "video/x-h265")) {
    parser_name = "h265parse";
    payloader_name = "rtph265pay";
    codec_text = "H.265 over RTP (payload type 96)";
  } else if (g_str_has_prefix (caps_name, "video/")) {
    gchar *detail = gst_caps_to_string (caps);

    post_stream_error (app,
        "The selected MP4 does not contain H.264 or H.265 video", detail);
    g_free (detail);
    gst_caps_unref (caps);
    return;
  } else {
    gst_caps_unref (caps);
    return;
  }

  gst_caps_unref (caps);

  if (!g_atomic_int_compare_and_exchange (&app->video_linked, 0, 1))
    return;

  queue = gst_element_factory_make ("queue", NULL);
  parser = gst_element_factory_make (parser_name, NULL);
  payloader = gst_element_factory_make (payloader_name, NULL);

  if (queue == NULL || parser == NULL || payloader == NULL) {
    if (queue != NULL)
      gst_object_unref (queue);
    if (parser != NULL)
      gst_object_unref (parser);
    if (payloader != NULL)
      gst_object_unref (payloader);
    g_atomic_int_set (&app->video_linked, 0);
    post_stream_error (app,
        "A required parser or RTP payloader is not installed",
        "Install the GStreamer good and bad plug-in packages for H.264/H.265");
    return;
  }

  g_object_set (payloader, "pt", 96, "mtu", 1200,
      "config-interval", -1, NULL);
  gst_bin_add_many (GST_BIN (app->pipeline), queue, parser, payloader, NULL);

  if (!gst_element_link_many (queue, parser, payloader, app->sink, NULL)) {
    post_stream_error (app, "Could not link the RTP sender chain",
        "queue -> parser -> RTP payloader -> udpgsosink failed");
    return;
  }

  queue_sink_pad = gst_element_get_static_pad (queue, "sink");
  pad_result = gst_pad_link (pad, queue_sink_pad);
  gst_object_unref (queue_sink_pad);
  if (pad_result != GST_PAD_LINK_OK) {
    post_stream_error (app, "Could not connect the MP4 video track",
        "qtdemux could not link its video pad to the RTP sender chain");
    return;
  }

  gst_element_sync_state_with_parent (queue);
  gst_element_sync_state_with_parent (parser);
  gst_element_sync_state_with_parent (payloader);
  queue_codec_text (app, codec_text);
}

static void
on_camera_pad_added (GstElement * decoder, GstPad * pad, gpointer user_data)
{
  DemoApp *app = user_data;
  GstCaps *caps;
  const GstStructure *structure;
  const gchar *caps_name;
  GstPad *queue_sink_pad;
  GstPadLinkReturn result;

  (void) decoder;

  caps = gst_pad_get_current_caps (pad);
  if (caps == NULL)
    caps = gst_pad_query_caps (pad, NULL);
  if (caps == NULL || gst_caps_is_empty (caps)) {
    if (caps != NULL)
      gst_caps_unref (caps);
    return;
  }

  structure = gst_caps_get_structure (caps, 0);
  caps_name = gst_structure_get_name (structure);
  if (!g_str_has_prefix (caps_name, "video/x-raw")) {
    gst_caps_unref (caps);
    return;
  }
  gst_caps_unref (caps);

  if (app->camera_queue == NULL ||
      !g_atomic_int_compare_and_exchange (&app->video_linked, 0, 1))
    return;

  queue_sink_pad = gst_element_get_static_pad (app->camera_queue, "sink");
  result = gst_pad_link (pad, queue_sink_pad);
  gst_object_unref (queue_sink_pad);

  if (result != GST_PAD_LINK_OK) {
    g_atomic_int_set (&app->video_linked, 0);
    post_stream_error (app, "Could not connect the USB camera video stream",
        "decodebin could not link its raw-video pad to the encoder chain");
    return;
  }

  queue_codec_text (app, "USB camera encoded as H.264/RTP (payload type 96)");
}

static gboolean
build_mp4_source (DemoApp * app, const gchar * filename)
{
  GstElement *source = gst_element_factory_make ("filesrc", "source");
  GstElement *demux = gst_element_factory_make ("qtdemux", "demux");

  if (source == NULL || demux == NULL) {
    if (source != NULL)
      gst_object_unref (source);
    if (demux != NULL)
      gst_object_unref (demux);
    gtk_label_set_text (GTK_LABEL (app->status_label),
        "filesrc or qtdemux is not installed.");
    return FALSE;
  }

  g_object_set (source, "location", filename, NULL);
  gst_bin_add_many (GST_BIN (app->pipeline), source, demux, NULL);
  if (!gst_element_link (source, demux)) {
    gtk_label_set_text (GTK_LABEL (app->status_label),
        "Could not link filesrc to qtdemux.");
    return FALSE;
  }

  g_signal_connect (demux, "pad-added", G_CALLBACK (on_demux_pad_added), app);
  gtk_label_set_text (GTK_LABEL (app->codec_label),
      "Detecting H.264/H.265 track...");
  gtk_label_set_text (GTK_LABEL (app->status_label), "Opening MP4 file...");
  return TRUE;
}

static void
get_camera_resolution (DemoApp * app, guint * width, guint * height)
{
  const gchar *resolution = gtk_combo_box_get_active_id (GTK_COMBO_BOX (
          app->camera_resolution_combo));

  if (g_strcmp0 (resolution, "1920x1080") == 0) {
    *width = 1920;
    *height = 1080;
  } else if (g_strcmp0 (resolution, "3840x2160") == 0) {
    *width = 3840;
    *height = 2160;
  } else {
    *width = 1280;
    *height = 720;
  }
}

static gboolean
build_camera_source (DemoApp * app, const gchar * device, guint width,
    guint height, guint framerate, guint bitrate)
{
  GstElement *elements[] = {
    gst_element_factory_make ("v4l2src", "camera-source"),
    gst_element_factory_make ("capsfilter", "camera-capture-caps"),
    gst_element_factory_make ("decodebin", "camera-decoder"),
    gst_element_factory_make ("queue", "camera-queue"),
    gst_element_factory_make ("videoconvert", "camera-convert"),
    gst_element_factory_make ("videoscale", "camera-scale"),
    gst_element_factory_make ("videorate", "camera-rate"),
    gst_element_factory_make ("capsfilter", "encoder-caps"),
    gst_element_factory_make ("x264enc", "camera-encoder"),
    gst_element_factory_make ("h264parse", "camera-parser"),
    gst_element_factory_make ("rtph264pay", "camera-payloader"),
  };
  GstElement *source = elements[0];
  GstElement *capture_filter = elements[1];
  GstElement *decoder = elements[2];
  GstElement *queue = elements[3];
  GstElement *output_filter = elements[7];
  GstElement *encoder = elements[8];
  GstElement *payloader = elements[10];
  GstCaps *capture_caps;
  GstCaps *jpeg_caps;
  GstCaps *output_caps;
  guint i;

  for (i = 0; i < G_N_ELEMENTS (elements); i++) {
    if (elements[i] == NULL) {
      guint j;

      for (j = 0; j < G_N_ELEMENTS (elements); j++) {
        if (elements[j] != NULL)
          gst_object_unref (elements[j]);
      }
      gtk_label_set_text (GTK_LABEL (app->status_label),
          "A camera element is missing. Install the Video4Linux, codec, "
          "good, bad and ugly GStreamer plug-in packages.");
      return FALSE;
    }
  }

  capture_caps = gst_caps_new_simple ("video/x-raw",
      "width", G_TYPE_INT, (gint) width,
      "height", G_TYPE_INT, (gint) height,
      "framerate", GST_TYPE_FRACTION, (gint) framerate, 1, NULL);
  jpeg_caps = gst_caps_new_simple ("image/jpeg",
      "width", G_TYPE_INT, (gint) width,
      "height", G_TYPE_INT, (gint) height,
      "framerate", GST_TYPE_FRACTION, (gint) framerate, 1, NULL);
  gst_caps_append (capture_caps, jpeg_caps);
  output_caps = gst_caps_new_simple ("video/x-raw",
      "format", G_TYPE_STRING, "I420",
      "width", G_TYPE_INT, (gint) width,
      "height", G_TYPE_INT, (gint) height,
      "framerate", GST_TYPE_FRACTION, (gint) framerate, 1, NULL);

  g_object_set (source, "device", device, "do-timestamp", TRUE, NULL);
  g_object_set (capture_filter, "caps", capture_caps, NULL);
  g_object_set (output_filter, "caps", output_caps, NULL);
  g_object_set (encoder,
      "bitrate", bitrate,
      "key-int-max", framerate,
      "bframes", 0,
      "byte-stream", TRUE, NULL);
  gst_util_set_object_arg (G_OBJECT (encoder), "tune", "zerolatency");
  gst_util_set_object_arg (G_OBJECT (encoder), "speed-preset", "ultrafast");
  g_object_set (payloader, "pt", 96, "mtu", 1200,
      "config-interval", -1, NULL);
  gst_caps_unref (capture_caps);
  gst_caps_unref (output_caps);

  for (i = 0; i < G_N_ELEMENTS (elements); i++)
    gst_bin_add (GST_BIN (app->pipeline), elements[i]);

  if (!gst_element_link_many (source, capture_filter, decoder, NULL) ||
      !gst_element_link_many (queue, elements[4], elements[5], elements[6],
          output_filter, encoder, elements[9], payloader, app->sink, NULL)) {
    gtk_label_set_text (GTK_LABEL (app->status_label),
        "Could not link the USB camera capture and H.264 encoder pipeline.");
    return FALSE;
  }

  app->camera_queue = queue;
  g_signal_connect (decoder, "pad-added", G_CALLBACK (on_camera_pad_added), app);
  gtk_label_set_text (GTK_LABEL (app->codec_label),
      "Waiting for the USB camera video stream...");
  gtk_label_set_text (GTK_LABEL (app->status_label), "Opening USB camera...");
  return TRUE;
}

static gboolean
update_statistics (gpointer user_data)
{
  DemoApp *app = user_data;
  guint64 packets = 0;
  guint64 bytes = 0;
  guint64 calls = 0;
  guint64 gso_batches = 0;
  guint64 fallbacks = 0;
  gboolean gso_supported = FALSE;
  gint64 now_us;
  gchar *text;
  gint64 position = GST_CLOCK_TIME_NONE;
  gint64 duration = GST_CLOCK_TIME_NONE;

  if (app->sink == NULL || app->pipeline == NULL)
    return G_SOURCE_REMOVE;

  g_object_get (app->sink,
      "packets-sent", &packets,
      "bytes-sent", &bytes,
      "system-calls", &calls,
      "gso-supported", &gso_supported,
      "gso-batches", &gso_batches,
      "fallback-count", &fallbacks, NULL);

  set_label_uint64 (app->packets_label, packets);
  text = g_format_size (bytes);
  gtk_label_set_text (GTK_LABEL (app->bytes_label), text);
  g_free (text);
  set_label_uint64 (app->calls_label, calls);
  set_label_uint64 (app->gso_batches_label, gso_batches);
  set_label_uint64 (app->fallbacks_label, fallbacks);
  gtk_label_set_text (GTK_LABEL (app->gso_support_label),
      gso_supported ? "Available" : "Unavailable");

  text = g_strdup_printf ("%.2f packets/call",
      calls == 0 ? 0.0 : (gdouble) packets / (gdouble) calls);
  gtk_label_set_text (GTK_LABEL (app->efficiency_label), text);
  g_free (text);

  now_us = g_get_monotonic_time ();
  if (app->previous_sample_us > 0 && now_us > app->previous_sample_us) {
    gdouble bits_per_second =
        (gdouble) (bytes - app->previous_bytes) * 8.0 * G_USEC_PER_SEC /
        (gdouble) (now_us - app->previous_sample_us);

    text = g_strdup_printf ("%.2f Mbit/s", bits_per_second / 1000000.0);
    gtk_label_set_text (GTK_LABEL (app->bitrate_label), text);
    g_free (text);
  }
  app->previous_bytes = bytes;
  app->previous_sample_us = now_us;

  if (gst_element_query_position (app->pipeline, GST_FORMAT_TIME, &position)) {
    gchar *position_text = format_clock_time ((GstClockTime) position);
    gchar *duration_text = NULL;

    if (gst_element_query_duration (app->pipeline, GST_FORMAT_TIME, &duration)
        && duration > 0) {
      duration_text = format_clock_time ((GstClockTime) duration);
      gtk_progress_bar_set_fraction (GTK_PROGRESS_BAR (app->progress),
          CLAMP ((gdouble) position / (gdouble) duration, 0.0, 1.0));
    } else {
      duration_text = g_strdup ("--:--");
      gtk_progress_bar_pulse (GTK_PROGRESS_BAR (app->progress));
    }

    text = g_strdup_printf ("%s / %s", position_text, duration_text);
    gtk_progress_bar_set_text (GTK_PROGRESS_BAR (app->progress), text);
    g_free (text);
    g_free (position_text);
    g_free (duration_text);
  }

  return G_SOURCE_CONTINUE;
}

static gboolean
on_bus_message (GstBus * bus, GstMessage * message, gpointer user_data)
{
  DemoApp *app = user_data;

  (void) bus;

  switch (GST_MESSAGE_TYPE (message)) {
    case GST_MESSAGE_ERROR:
    {
      GError *error = NULL;
      gchar *debug = NULL;
      gchar *status;

      gst_message_parse_error (message, &error, &debug);
      status = g_strdup_printf ("Error: %s", error->message);
      app->bus_watch_id = 0;
      stop_pipeline (app, status);
      g_free (status);
      g_clear_error (&error);
      g_free (debug);
      return G_SOURCE_REMOVE;
    }
    case GST_MESSAGE_EOS:
      app->bus_watch_id = 0;
      stop_pipeline (app,
          "Input completed. The UDP receiver remains active until it is stopped.");
      return G_SOURCE_REMOVE;
    case GST_MESSAGE_STATE_CHANGED:
      if (GST_MESSAGE_SRC (message) == GST_OBJECT (app->pipeline)) {
        GstState old_state;
        GstState new_state;
        GstState pending_state;

        gst_message_parse_state_changed (message, &old_state, &new_state,
            &pending_state);
        if (new_state == GST_STATE_PLAYING)
          gtk_label_set_text (GTK_LABEL (app->status_label), "Streaming");
      }
      break;
    default:
      break;
  }

  return G_SOURCE_CONTINUE;
}

static gboolean
start_pipeline (DemoApp * app)
{
  gchar *filename = NULL;
  const gchar *input_type;
  const gchar *host;
  const gchar *mode;
  const gchar *camera_device;
  guint port;
  guint batch_size;
  guint camera_width = 1280;
  guint camera_height = 720;
  guint camera_framerate = 30;
  guint camera_bitrate = 4000;
  gboolean fallback;
  gboolean source_ready;
  GstBus *bus;
  GstStateChangeReturn state_result;

  input_type = gtk_combo_box_get_active_id (GTK_COMBO_BOX (app->input_combo));
  if (g_strcmp0 (input_type, "camera") == 0) {
    camera_device = gtk_entry_get_text (GTK_ENTRY (app->camera_device_entry));
    if (camera_device == NULL || *camera_device == '\0') {
      gtk_label_set_text (GTK_LABEL (app->status_label),
          "Enter a Video4Linux device such as /dev/video0.");
      return FALSE;
    }
    if (!g_file_test (camera_device, G_FILE_TEST_EXISTS)) {
      gtk_label_set_text (GTK_LABEL (app->status_label),
          "The selected /dev/video* camera device does not exist.");
      return FALSE;
    }
    get_camera_resolution (app, &camera_width, &camera_height);
    camera_framerate = gtk_spin_button_get_value_as_int (GTK_SPIN_BUTTON (
            app->camera_framerate_spin));
    camera_bitrate = gtk_spin_button_get_value_as_int (GTK_SPIN_BUTTON (
            app->camera_bitrate_spin));
  } else {
    filename = gtk_file_chooser_get_filename (GTK_FILE_CHOOSER (
            app->file_chooser));
    if (filename == NULL) {
      gtk_label_set_text (GTK_LABEL (app->status_label),
          "Select an MP4 file first.");
      return FALSE;
    }
  }

  host = gtk_entry_get_text (GTK_ENTRY (app->host_entry));
  if (host == NULL || *host == '\0') {
    gtk_label_set_text (GTK_LABEL (app->status_label),
        "Enter a destination address.");
    g_free (filename);
    return FALSE;
  }

  port = gtk_spin_button_get_value_as_int (GTK_SPIN_BUTTON (app->port_spin));
  batch_size = gtk_spin_button_get_value_as_int (GTK_SPIN_BUTTON (
          app->batch_spin));
  fallback = gtk_toggle_button_get_active (GTK_TOGGLE_BUTTON (
          app->fallback_check));
  mode = gtk_combo_box_get_active_id (GTK_COMBO_BOX (app->mode_combo));

  app->pipeline = gst_pipeline_new ("udpgso-demo-sender");
  app->sink = gst_element_factory_make ("udpgsosink", "network-sink");

  if (app->pipeline == NULL || app->sink == NULL) {
    gtk_label_set_text (GTK_LABEL (app->status_label),
        app->sink == NULL ?
        "udpgsosink was not found. Set GST_PLUGIN_PATH to the build/src directory."
        : "Could not create the required GStreamer elements.");
    if (app->sink != NULL)
      gst_object_unref (app->sink);
    if (app->pipeline != NULL)
      gst_object_unref (app->pipeline);
    app->pipeline = NULL;
    app->sink = NULL;
    g_free (filename);
    return FALSE;
  }

  g_object_set (app->sink,
      "host", host,
      "port", port,
      "batch-size", batch_size,
      "fallback", fallback,
      "sync", TRUE, NULL);
  gst_util_set_object_arg (G_OBJECT (app->sink), "io-mode",
      mode != NULL ? mode : "auto");

  if (!gst_bin_add (GST_BIN (app->pipeline), app->sink)) {
    gtk_label_set_text (GTK_LABEL (app->status_label),
        "Could not add udpgsosink to the sender pipeline.");
    gst_object_unref (app->sink);
    gst_object_unref (app->pipeline);
    app->pipeline = NULL;
    app->sink = NULL;
    g_free (filename);
    return FALSE;
  }

  g_atomic_int_set (&app->video_linked, 0);
  app->camera_queue = NULL;
  if (g_strcmp0 (input_type, "camera") == 0) {
    source_ready = build_camera_source (app, camera_device, camera_width,
        camera_height, camera_framerate, camera_bitrate);
  } else {
    source_ready = build_mp4_source (app, filename);
  }
  if (!source_ready) {
    gst_element_set_state (app->pipeline, GST_STATE_NULL);
    gst_object_unref (app->pipeline);
    app->pipeline = NULL;
    app->sink = NULL;
    app->camera_queue = NULL;
    g_free (filename);
    return FALSE;
  }

  bus = gst_element_get_bus (app->pipeline);
  app->bus_watch_id = gst_bus_add_watch (bus, on_bus_message, app);
  gst_object_unref (bus);

  app->previous_bytes = 0;
  app->previous_sample_us = 0;
  gtk_label_set_text (GTK_LABEL (app->packets_label), "0");
  gtk_label_set_text (GTK_LABEL (app->bytes_label), "0 bytes");
  gtk_label_set_text (GTK_LABEL (app->calls_label), "0");
  gtk_label_set_text (GTK_LABEL (app->efficiency_label), "0.00 packets/call");
  gtk_label_set_text (GTK_LABEL (app->bitrate_label), "0.00 Mbit/s");
  gtk_label_set_text (GTK_LABEL (app->gso_support_label), "Probing...");
  gtk_label_set_text (GTK_LABEL (app->gso_batches_label), "0");
  gtk_label_set_text (GTK_LABEL (app->fallbacks_label), "0");
  app->stats_timer_id = g_timeout_add (500, update_statistics, app);
  gtk_progress_bar_set_fraction (GTK_PROGRESS_BAR (app->progress), 0.0);
  gtk_progress_bar_set_text (GTK_PROGRESS_BAR (app->progress), "00:00 / --:--");
  set_controls_running (app, TRUE);

  state_result = gst_element_set_state (app->pipeline, GST_STATE_PLAYING);
  if (state_result == GST_STATE_CHANGE_FAILURE) {
    stop_pipeline (app, "Could not start the sender pipeline.");
    g_free (filename);
    return FALSE;
  }

  g_free (filename);
  return TRUE;
}

static void
stop_pipeline (DemoApp * app, const gchar * final_status)
{
  if (app->stats_timer_id != 0) {
    g_source_remove (app->stats_timer_id);
    app->stats_timer_id = 0;
  }
  if (app->bus_watch_id != 0) {
    g_source_remove (app->bus_watch_id);
    app->bus_watch_id = 0;
  }
  if (app->pipeline != NULL) {
    gst_element_set_state (app->pipeline, GST_STATE_NULL);
    gst_object_unref (app->pipeline);
    app->pipeline = NULL;
    app->sink = NULL;
    app->camera_queue = NULL;
  }

  g_atomic_int_set (&app->video_linked, 0);
  set_controls_running (app, FALSE);
  if (final_status != NULL)
    gtk_label_set_text (GTK_LABEL (app->status_label), final_status);
}

static void
on_start_clicked (GtkButton * button, gpointer user_data)
{
  (void) button;
  start_pipeline ((DemoApp *) user_data);
}

static void
on_stop_clicked (GtkButton * button, gpointer user_data)
{
  (void) button;
  stop_pipeline ((DemoApp *) user_data, "Stopped by user.");
}

static void
on_window_destroy (GtkWidget * widget, gpointer user_data)
{
  DemoApp *app = user_data;

  (void) widget;
  stop_pipeline (app, NULL);
  gtk_main_quit ();
}

static GtkWidget *
make_value_label (void)
{
  GtkWidget *label = gtk_label_new ("0");

  gtk_widget_set_halign (label, GTK_ALIGN_START);
  gtk_widget_set_hexpand (label, TRUE);
  return label;
}

static void
add_grid_row (GtkGrid * grid, gint row, const gchar * title, GtkWidget * value)
{
  GtkWidget *label = gtk_label_new (title);

  gtk_widget_set_halign (label, GTK_ALIGN_END);
  gtk_style_context_add_class (gtk_widget_get_style_context (label),
      "dim-label");
  gtk_grid_attach (grid, label, 0, row, 1, 1);
  gtk_grid_attach (grid, value, 1, row, 1, 1);
}

static void
on_input_changed (GtkComboBox * combo, gpointer user_data)
{
  DemoApp *app = user_data;
  const gchar *input_type = gtk_combo_box_get_active_id (combo);

  gtk_stack_set_visible_child_name (GTK_STACK (app->input_stack),
      g_strcmp0 (input_type, "camera") == 0 ? "camera" : "mp4");
  gtk_label_set_text (GTK_LABEL (app->codec_label), "No stream selected");
  gtk_label_set_text (GTK_LABEL (app->status_label),
      g_strcmp0 (input_type, "camera") == 0 ?
      "Ready for USB camera input (RTP/H.264)." :
      "Ready for MP4 file input (RTP/H.264 or RTP/H.265).");
}

static GtkWidget *
build_ui (DemoApp * app)
{
  GtkWidget *window;
  GtkWidget *scroller;
  GtkWidget *outer;
  GtkWidget *title;
  GtkWidget *description;
  GtkWidget *settings_frame;
  GtkWidget *settings;
  GtkWidget *mp4_settings;
  GtkWidget *camera_settings;
  GtkWidget *label;
  GtkFileFilter *mp4_filter;
  GtkAdjustment *port_adjustment;
  GtkAdjustment *batch_adjustment;
  GtkAdjustment *framerate_adjustment;
  GtkAdjustment *bitrate_adjustment;
  GtkWidget *button_box;
  GtkWidget *status_frame;
  GtkWidget *status_box;
  GtkWidget *stats_frame;
  GtkWidget *stats;
  GtkWidget *gso_note;

  window = gtk_window_new (GTK_WINDOW_TOPLEVEL);
  gtk_window_set_title (GTK_WINDOW (window), "UDP GSO Video Transmitter");
  gtk_window_set_default_size (GTK_WINDOW (window), 760, 700);
  gtk_container_set_border_width (GTK_CONTAINER (window), 18);

  scroller = gtk_scrolled_window_new (NULL, NULL);
  gtk_scrolled_window_set_policy (GTK_SCROLLED_WINDOW (scroller),
      GTK_POLICY_NEVER, GTK_POLICY_AUTOMATIC);
  gtk_container_add (GTK_CONTAINER (window), scroller);

  outer = gtk_box_new (GTK_ORIENTATION_VERTICAL, 14);
  gtk_container_add (GTK_CONTAINER (scroller), outer);

  title = gtk_label_new (NULL);
  gtk_label_set_markup (GTK_LABEL (title),
      "<span size='x-large' weight='bold'>UDP GSO Video Transmitter</span>");
  gtk_widget_set_halign (title, GTK_ALIGN_START);
  gtk_box_pack_start (GTK_BOX (outer), title, FALSE, FALSE, 0);

  description = gtk_label_new (
      "Streams video from an MP4 file or USB camera as RTP. "
      "Start a matching udpsrc receiver before starting transmission.");
  gtk_label_set_line_wrap (GTK_LABEL (description), TRUE);
  gtk_widget_set_halign (description, GTK_ALIGN_START);
  gtk_box_pack_start (GTK_BOX (outer), description, FALSE, FALSE, 0);

  settings_frame = gtk_frame_new ("Transmission settings");
  gtk_box_pack_start (GTK_BOX (outer), settings_frame, FALSE, FALSE, 0);
  settings = gtk_grid_new ();
  gtk_grid_set_row_spacing (GTK_GRID (settings), 10);
  gtk_grid_set_column_spacing (GTK_GRID (settings), 12);
  gtk_container_set_border_width (GTK_CONTAINER (settings), 12);
  gtk_container_add (GTK_CONTAINER (settings_frame), settings);

  label = gtk_label_new ("Input type");
  gtk_widget_set_halign (label, GTK_ALIGN_END);
  gtk_grid_attach (GTK_GRID (settings), label, 0, 0, 1, 1);
  app->input_combo = gtk_combo_box_text_new ();
  gtk_combo_box_text_append (GTK_COMBO_BOX_TEXT (app->input_combo), "mp4",
      "MP4 file");
  gtk_combo_box_text_append (GTK_COMBO_BOX_TEXT (app->input_combo), "camera",
      "USB camera");
  gtk_combo_box_set_active_id (GTK_COMBO_BOX (app->input_combo), "mp4");
  gtk_grid_attach (GTK_GRID (settings), app->input_combo, 1, 0, 3, 1);

  app->input_stack = gtk_stack_new ();
  gtk_stack_set_transition_type (GTK_STACK (app->input_stack),
      GTK_STACK_TRANSITION_TYPE_CROSSFADE);
  gtk_widget_set_hexpand (app->input_stack, TRUE);

  mp4_settings = gtk_grid_new ();
  gtk_grid_set_column_spacing (GTK_GRID (mp4_settings), 12);
  label = gtk_label_new ("MP4 file");
  gtk_widget_set_halign (label, GTK_ALIGN_END);
  gtk_grid_attach (GTK_GRID (mp4_settings), label, 0, 0, 1, 1);
  app->file_chooser = gtk_file_chooser_button_new ("Select an MP4 file",
      GTK_FILE_CHOOSER_ACTION_OPEN);
  gtk_widget_set_hexpand (app->file_chooser, TRUE);
  mp4_filter = gtk_file_filter_new ();
  gtk_file_filter_set_name (mp4_filter, "MP4 video files");
  gtk_file_filter_add_mime_type (mp4_filter, "video/mp4");
  gtk_file_filter_add_pattern (mp4_filter, "*.mp4");
  gtk_file_filter_add_pattern (mp4_filter, "*.MP4");
  gtk_file_chooser_add_filter (GTK_FILE_CHOOSER (app->file_chooser), mp4_filter);
  gtk_grid_attach (GTK_GRID (mp4_settings), app->file_chooser, 1, 0, 3, 1);
  gtk_stack_add_named (GTK_STACK (app->input_stack), mp4_settings, "mp4");

  camera_settings = gtk_grid_new ();
  gtk_grid_set_row_spacing (GTK_GRID (camera_settings), 8);
  gtk_grid_set_column_spacing (GTK_GRID (camera_settings), 12);
  label = gtk_label_new ("Device");
  gtk_widget_set_halign (label, GTK_ALIGN_END);
  gtk_grid_attach (GTK_GRID (camera_settings), label, 0, 0, 1, 1);
  app->camera_device_entry = gtk_entry_new ();
  gtk_entry_set_text (GTK_ENTRY (app->camera_device_entry), "/dev/video0");
  gtk_entry_set_placeholder_text (GTK_ENTRY (app->camera_device_entry),
      "/dev/video0");
  gtk_widget_set_hexpand (app->camera_device_entry, TRUE);
  gtk_grid_attach (GTK_GRID (camera_settings), app->camera_device_entry,
      1, 0, 3, 1);

  label = gtk_label_new ("Resolution");
  gtk_widget_set_halign (label, GTK_ALIGN_END);
  gtk_grid_attach (GTK_GRID (camera_settings), label, 0, 1, 1, 1);
  app->camera_resolution_combo = gtk_combo_box_text_new ();
  gtk_combo_box_text_append (GTK_COMBO_BOX_TEXT (
          app->camera_resolution_combo), "1280x720", "1280 x 720");
  gtk_combo_box_text_append (GTK_COMBO_BOX_TEXT (
          app->camera_resolution_combo), "1920x1080", "1920 x 1080");
  gtk_combo_box_text_append (GTK_COMBO_BOX_TEXT (
          app->camera_resolution_combo), "3840x2160", "3840 x 2160");
  gtk_combo_box_set_active_id (GTK_COMBO_BOX (
          app->camera_resolution_combo), "1280x720");
  gtk_grid_attach (GTK_GRID (camera_settings),
      app->camera_resolution_combo, 1, 1, 1, 1);

  label = gtk_label_new ("Frame rate");
  gtk_widget_set_halign (label, GTK_ALIGN_END);
  gtk_grid_attach (GTK_GRID (camera_settings), label, 2, 1, 1, 1);
  framerate_adjustment = gtk_adjustment_new (30, 1, 60, 1, 5, 0);
  app->camera_framerate_spin = gtk_spin_button_new (
      framerate_adjustment, 1, 0);
  gtk_grid_attach (GTK_GRID (camera_settings),
      app->camera_framerate_spin, 3, 1, 1, 1);

  label = gtk_label_new ("H.264 bitrate (kbit/s)");
  gtk_widget_set_halign (label, GTK_ALIGN_END);
  gtk_grid_attach (GTK_GRID (camera_settings), label, 0, 2, 1, 1);
  bitrate_adjustment = gtk_adjustment_new (4000, 250, 50000, 250, 1000, 0);
  app->camera_bitrate_spin = gtk_spin_button_new (bitrate_adjustment, 1, 0);
  gtk_grid_attach (GTK_GRID (camera_settings),
      app->camera_bitrate_spin, 1, 2, 1, 1);
  gtk_stack_add_named (GTK_STACK (app->input_stack), camera_settings, "camera");
  gtk_stack_set_visible_child_name (GTK_STACK (app->input_stack), "mp4");
  gtk_grid_attach (GTK_GRID (settings), app->input_stack, 0, 1, 4, 1);

  label = gtk_label_new ("Destination");
  gtk_widget_set_halign (label, GTK_ALIGN_END);
  gtk_grid_attach (GTK_GRID (settings), label, 0, 2, 1, 1);
  app->host_entry = gtk_entry_new ();
  gtk_entry_set_text (GTK_ENTRY (app->host_entry), "127.0.0.1");
  gtk_entry_set_placeholder_text (GTK_ENTRY (app->host_entry),
      "Receiver IP address");
  gtk_widget_set_hexpand (app->host_entry, TRUE);
  gtk_grid_attach (GTK_GRID (settings), app->host_entry, 1, 2, 1, 1);

  label = gtk_label_new ("Port");
  gtk_widget_set_halign (label, GTK_ALIGN_END);
  gtk_grid_attach (GTK_GRID (settings), label, 2, 2, 1, 1);
  port_adjustment = gtk_adjustment_new (5004, 1, 65535, 1, 100, 0);
  app->port_spin = gtk_spin_button_new (port_adjustment, 1, 0);
  gtk_grid_attach (GTK_GRID (settings), app->port_spin, 3, 2, 1, 1);

  label = gtk_label_new ("I/O mode");
  gtk_widget_set_halign (label, GTK_ALIGN_END);
  gtk_grid_attach (GTK_GRID (settings), label, 0, 3, 1, 1);
  app->mode_combo = gtk_combo_box_text_new ();
  gtk_combo_box_text_append (GTK_COMBO_BOX_TEXT (app->mode_combo), "auto",
      "Automatic");
  gtk_combo_box_text_append (GTK_COMBO_BOX_TEXT (app->mode_combo), "sendmsg",
      "sendmsg baseline");
  gtk_combo_box_text_append (GTK_COMBO_BOX_TEXT (app->mode_combo), "sendmmsg",
      "sendmmsg batching");
  gtk_combo_box_text_append (GTK_COMBO_BOX_TEXT (app->mode_combo), "gso",
      "UDP GSO");
  gtk_combo_box_set_active_id (GTK_COMBO_BOX (app->mode_combo), "auto");
  gtk_grid_attach (GTK_GRID (settings), app->mode_combo, 1, 3, 1, 1);

  label = gtk_label_new ("Batch size");
  gtk_widget_set_halign (label, GTK_ALIGN_END);
  gtk_grid_attach (GTK_GRID (settings), label, 2, 3, 1, 1);
  batch_adjustment = gtk_adjustment_new (32, 1, 1024, 1, 16, 0);
  app->batch_spin = gtk_spin_button_new (batch_adjustment, 1, 0);
  gtk_grid_attach (GTK_GRID (settings), app->batch_spin, 3, 3, 1, 1);

  app->fallback_check = gtk_check_button_new_with_label (
      "Fall back to sendmmsg when GSO is unavailable");
  gtk_toggle_button_set_active (GTK_TOGGLE_BUTTON (app->fallback_check), TRUE);
  gtk_grid_attach (GTK_GRID (settings), app->fallback_check, 1, 4, 3, 1);

  button_box = gtk_button_box_new (GTK_ORIENTATION_HORIZONTAL);
  gtk_button_box_set_layout (GTK_BUTTON_BOX (button_box), GTK_BUTTONBOX_END);
  gtk_box_set_spacing (GTK_BOX (button_box), 8);
  app->start_button = gtk_button_new_with_label ("Start streaming");
  gtk_style_context_add_class (gtk_widget_get_style_context (app->start_button),
      "suggested-action");
  app->stop_button = gtk_button_new_with_label ("Stop");
  gtk_container_add (GTK_CONTAINER (button_box), app->start_button);
  gtk_container_add (GTK_CONTAINER (button_box), app->stop_button);
  gtk_box_pack_start (GTK_BOX (outer), button_box, FALSE, FALSE, 0);

  status_frame = gtk_frame_new ("Stream state");
  gtk_box_pack_start (GTK_BOX (outer), status_frame, FALSE, FALSE, 0);
  status_box = gtk_box_new (GTK_ORIENTATION_VERTICAL, 7);
  gtk_container_set_border_width (GTK_CONTAINER (status_box), 12);
  gtk_container_add (GTK_CONTAINER (status_frame), status_box);
  app->status_label = gtk_label_new ("Ready");
  gtk_widget_set_halign (app->status_label, GTK_ALIGN_START);
  gtk_label_set_line_wrap (GTK_LABEL (app->status_label), TRUE);
  gtk_box_pack_start (GTK_BOX (status_box), app->status_label, FALSE, FALSE, 0);
  app->codec_label = gtk_label_new ("No stream selected");
  gtk_widget_set_halign (app->codec_label, GTK_ALIGN_START);
  gtk_box_pack_start (GTK_BOX (status_box), app->codec_label, FALSE, FALSE, 0);
  app->progress = gtk_progress_bar_new ();
  gtk_progress_bar_set_show_text (GTK_PROGRESS_BAR (app->progress), TRUE);
  gtk_progress_bar_set_text (GTK_PROGRESS_BAR (app->progress), "00:00 / --:--");
  gtk_box_pack_start (GTK_BOX (status_box), app->progress, FALSE, FALSE, 0);

  stats_frame = gtk_frame_new ("Live udpgsosink statistics");
  gtk_box_pack_start (GTK_BOX (outer), stats_frame, TRUE, TRUE, 0);
  stats = gtk_grid_new ();
  gtk_grid_set_row_spacing (GTK_GRID (stats), 7);
  gtk_grid_set_column_spacing (GTK_GRID (stats), 12);
  gtk_container_set_border_width (GTK_CONTAINER (stats), 12);
  gtk_container_add (GTK_CONTAINER (stats_frame), stats);

  app->packets_label = make_value_label ();
  app->bytes_label = make_value_label ();
  app->calls_label = make_value_label ();
  app->efficiency_label = make_value_label ();
  app->bitrate_label = make_value_label ();
  app->gso_support_label = make_value_label ();
  app->gso_batches_label = make_value_label ();
  app->fallbacks_label = make_value_label ();
  add_grid_row (GTK_GRID (stats), 0, "Packets sent", app->packets_label);
  add_grid_row (GTK_GRID (stats), 1, "Payload bytes", app->bytes_label);
  add_grid_row (GTK_GRID (stats), 2, "Network system calls", app->calls_label);
  add_grid_row (GTK_GRID (stats), 3, "Submission efficiency",
      app->efficiency_label);
  add_grid_row (GTK_GRID (stats), 4, "Current payload rate",
      app->bitrate_label);
  add_grid_row (GTK_GRID (stats), 5, "Kernel UDP GSO",
      app->gso_support_label);
  add_grid_row (GTK_GRID (stats), 6, "Successful GSO batches",
      app->gso_batches_label);
  add_grid_row (GTK_GRID (stats), 7, "GSO fallbacks", app->fallbacks_label);

  gso_note = gtk_label_new (
      "A zero GSO-batch count means upstream delivered individual RTP buffers; "
      "it does not mean that video transmission failed.");
  gtk_label_set_line_wrap (GTK_LABEL (gso_note), TRUE);
  gtk_widget_set_halign (gso_note, GTK_ALIGN_START);
  gtk_style_context_add_class (gtk_widget_get_style_context (gso_note),
      "dim-label");
  gtk_grid_attach (GTK_GRID (stats), gso_note, 0, 8, 2, 1);

  g_signal_connect (app->start_button, "clicked",
      G_CALLBACK (on_start_clicked), app);
  g_signal_connect (app->stop_button, "clicked",
      G_CALLBACK (on_stop_clicked), app);
  g_signal_connect (app->input_combo, "changed",
      G_CALLBACK (on_input_changed), app);
  g_signal_connect (window, "destroy", G_CALLBACK (on_window_destroy), app);

  set_controls_running (app, FALSE);
  return window;
}

int
main (int argc, char **argv)
{
  DemoApp app = { 0 };

  gtk_init (&argc, &argv);
  gst_init (&argc, &argv);

  app.window = build_ui (&app);
  gtk_widget_show_all (app.window);
  gtk_main ();

  return 0;
}

