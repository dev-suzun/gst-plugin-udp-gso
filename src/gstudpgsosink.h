/* SPDX-License-Identifier: BSD-3-Clause */

#pragma once

#include <gst/base/gstbasesink.h>

G_BEGIN_DECLS

#define GST_TYPE_UDP_GSO_SINK (gst_udp_gso_sink_get_type ())
G_DECLARE_FINAL_TYPE (GstUdpGsoSink, gst_udp_gso_sink, GST, UDP_GSO_SINK,
    GstBaseSink)

G_END_DECLS

