/* SPDX-License-Identifier: BSD-3-Clause */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct
{
  uint64_t next_txtime_ns;
  uint64_t rate_bps;
  uint64_t lead_time_ns;
  uint32_t wire_overhead_bytes;
} UdpGsoSchedule;

typedef struct
{
  bool valid;
  uint16_t sequence;
  uint32_t timestamp;
  bool marker;
} UdpGsoRtpInfo;

/* Return the rounded-up serialization interval, or zero when rate_bps is 0. */
uint64_t udpgso_schedule_interval_ns (size_t payload_bytes,
                                     uint32_t wire_overhead_bytes,
                                     uint64_t rate_bps);

void udpgso_schedule_reset (UdpGsoSchedule *schedule);

/*
 * Return a transmission time no earlier than now + lead time and advance the
 * schedule by the packet's serialization interval.
 */
uint64_t udpgso_schedule_next (UdpGsoSchedule *schedule,
                              uint64_t now_ns,
                              size_t payload_bytes);

/* True when adding @next would cross a validated RTP frame boundary. */
bool udpgso_rtp_starts_new_batch (const UdpGsoRtpInfo *previous,
                                  const UdpGsoRtpInfo *next);

