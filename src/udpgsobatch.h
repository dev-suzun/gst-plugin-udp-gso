/* SPDX-License-Identifier: BSD-3-Clause */

#pragma once

#include <stddef.h>

/*
 * Return the number of consecutive packets, starting at @start, that have
 * exactly the same non-zero payload size.  The result is capped at
 * @max_segments.  A zero-length packet forms a run of one because Linux UDP
 * GSO cannot use a zero segment size.
 */
size_t udpgso_equal_size_run (const size_t *sizes,
                              size_t count,
                              size_t start,
                              size_t max_segments);

/*
 * Limit a GSO run so its aggregate payload does not exceed @max_payload_size.
 * A zero segment size, zero limit, or oversized individual segment returns 0.
 */
size_t udpgso_limit_segments_by_payload (size_t segment_size,
                                         size_t max_segments,
                                         size_t max_payload_size);
