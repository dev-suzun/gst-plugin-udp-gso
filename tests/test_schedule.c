/* SPDX-License-Identifier: BSD-3-Clause */

#include "udpgsoschedule.h"

#include <assert.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>

int
main (void)
{
  UdpGsoSchedule schedule = {
    .rate_bps = UINT64_C (100000000),
    .lead_time_ns = UINT64_C (2000000),
    .wire_overhead_bytes = 42,
  };
  UdpGsoRtpInfo first = {
    .valid = true,
    .sequence = 10,
    .timestamp = 90000,
    .marker = false,
  };
  UdpGsoRtpInfo same_frame = {
    .valid = true,
    .sequence = 11,
    .timestamp = 90000,
    .marker = false,
  };
  UdpGsoRtpInfo marked = {
    .valid = true,
    .sequence = 12,
    .timestamp = 90000,
    .marker = true,
  };
  UdpGsoRtpInfo next_frame = {
    .valid = true,
    .sequence = 13,
    .timestamp = 93000,
    .marker = false,
  };
  UdpGsoRtpInfo invalid = { 0 };
  uint64_t first_txtime;
  uint64_t second_txtime;

  assert (udpgso_schedule_interval_ns (1200, 50, 100000000) == 100000);
  assert (udpgso_schedule_interval_ns (1, 0, 3000000000) == 3);
  assert (udpgso_schedule_interval_ns (1200, 42, 0) == 0);
  assert (udpgso_schedule_interval_ns (SIZE_MAX, UINT32_MAX, 1) ==
      UINT64_MAX);

  first_txtime = udpgso_schedule_next (&schedule, 10000000, 1200);
  second_txtime = udpgso_schedule_next (&schedule, 10001000, 1200);
  assert (first_txtime == 12000000);
  assert (second_txtime == 12099360);
  assert (udpgso_schedule_next (NULL, 42, 1200) == 42);

  udpgso_schedule_reset (&schedule);
  assert (schedule.next_txtime_ns == 0);

  assert (!udpgso_rtp_starts_new_batch (&first, &same_frame));
  assert (udpgso_rtp_starts_new_batch (&marked, &same_frame));
  assert (udpgso_rtp_starts_new_batch (&marked, &next_frame));
  assert (udpgso_rtp_starts_new_batch (&same_frame, &next_frame));
  assert (!udpgso_rtp_starts_new_batch (&invalid, &next_frame));
  assert (!udpgso_rtp_starts_new_batch (NULL, &next_frame));

  return 0;
}

