/* SPDX-License-Identifier: BSD-3-Clause */

#include <gst/gst.h>

#include "gstudpgsosink.h"

#ifndef PACKAGE
#define PACKAGE "gst-plugin-udp-gso"
#endif

static gboolean
plugin_init (GstPlugin *plugin)
{
  return gst_element_register (plugin, "udpgsosink", GST_RANK_NONE,
      GST_TYPE_UDP_GSO_SINK);
}

GST_PLUGIN_DEFINE (
    GST_VERSION_MAJOR,
    GST_VERSION_MINOR,
    udpgso,
    "Linux high-performance UDP sink with batching, GSO, and packet pacing",
    plugin_init,
    "0.2.0",
    "BSD",
    "gst-plugin-udp-gso",
    "https://gstreamer.freedesktop.org/")

