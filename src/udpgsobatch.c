/* SPDX-License-Identifier: BSD-3-Clause */

#include "udpgsobatch.h"

size_t
udpgso_equal_size_run (const size_t *sizes,
                       size_t count,
                       size_t start,
                       size_t max_segments)
{
  size_t run;
  size_t segment_size;

  if (sizes == NULL || start >= count || max_segments == 0)
    return 0;

  segment_size = sizes[start];
  if (segment_size == 0)
    return 1;

  run = 1;
  while (start + run < count &&
         run < max_segments &&
         sizes[start + run] == segment_size) {
    run++;
  }

  return run;
}

size_t
udpgso_limit_segments_by_payload (size_t segment_size,
                                  size_t max_segments,
                                  size_t max_payload_size)
{
  size_t payload_segments;

  if (segment_size == 0 || max_segments == 0 ||
      segment_size > max_payload_size)
    return 0;

  payload_segments = max_payload_size / segment_size;
  return payload_segments < max_segments ? payload_segments : max_segments;
}
