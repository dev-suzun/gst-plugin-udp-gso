/* SPDX-License-Identifier: BSD-3-Clause */

#include "udpgsoschedule.h"

#include <limits.h>

#define NANOSECONDS_PER_SECOND UINT64_C (1000000000)

static uint64_t
udpgso_saturating_add_u64 (uint64_t left, uint64_t right)
{
  if (UINT64_MAX - left < right)
    return UINT64_MAX;
  return left + right;
}

uint64_t
udpgso_schedule_interval_ns (size_t payload_bytes,
                             uint32_t wire_overhead_bytes,
                             uint64_t rate_bps)
{
  __uint128_t wire_bytes;
  __uint128_t numerator;
  __uint128_t interval;

  if (rate_bps == 0)
    return 0;

  wire_bytes = (__uint128_t) payload_bytes + wire_overhead_bytes;
  numerator = wire_bytes * 8 * NANOSECONDS_PER_SECOND;
  interval = (numerator + rate_bps - 1) / rate_bps;

  return interval > UINT64_MAX ? UINT64_MAX : (uint64_t) interval;
}

void
udpgso_schedule_reset (UdpGsoSchedule *schedule)
{
  if (schedule != NULL)
    schedule->next_txtime_ns = 0;
}

uint64_t
udpgso_schedule_next (UdpGsoSchedule *schedule,
                      uint64_t now_ns,
                      size_t payload_bytes)
{
  uint64_t earliest;
  uint64_t txtime;
  uint64_t interval;

  if (schedule == NULL)
    return now_ns;

  earliest = udpgso_saturating_add_u64 (now_ns, schedule->lead_time_ns);
  txtime = schedule->next_txtime_ns > earliest ?
      schedule->next_txtime_ns : earliest;
  interval = udpgso_schedule_interval_ns (payload_bytes,
      schedule->wire_overhead_bytes, schedule->rate_bps);
  schedule->next_txtime_ns = udpgso_saturating_add_u64 (txtime, interval);

  return txtime;
}

bool
udpgso_rtp_starts_new_batch (const UdpGsoRtpInfo *previous,
                             const UdpGsoRtpInfo *next)
{
  if (previous == NULL || next == NULL || !previous->valid || !next->valid)
    return false;

  return previous->marker || previous->timestamp != next->timestamp;
}

