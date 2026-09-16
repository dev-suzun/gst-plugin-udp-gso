/* SPDX-License-Identifier: BSD-3-Clause */

#include "udpgsobatch.h"

#include <assert.h>
#include <stddef.h>

int
main (void)
{
  const size_t sizes[] = { 1200, 1200, 1200, 900, 1200, 1200, 0 };

  assert (udpgso_equal_size_run (sizes, 7, 0, 64) == 3);
  assert (udpgso_equal_size_run (sizes, 7, 0, 2) == 2);
  assert (udpgso_equal_size_run (sizes, 7, 3, 64) == 1);
  assert (udpgso_equal_size_run (sizes, 7, 4, 64) == 2);
  assert (udpgso_equal_size_run (sizes, 7, 6, 64) == 1);
  assert (udpgso_equal_size_run (sizes, 7, 7, 64) == 0);
  assert (udpgso_equal_size_run (NULL, 7, 0, 64) == 0);
  assert (udpgso_equal_size_run (sizes, 7, 0, 0) == 0);

  assert (udpgso_limit_segments_by_payload (1200, 64, 65507) == 54);
  assert (udpgso_limit_segments_by_payload (1400, 64, 65507) == 46);
  assert (udpgso_limit_segments_by_payload (1200, 32, 65507) == 32);
  assert (udpgso_limit_segments_by_payload (65507, 64, 65507) == 1);
  assert (udpgso_limit_segments_by_payload (65508, 64, 65507) == 0);
  assert (udpgso_limit_segments_by_payload (0, 64, 65507) == 0);
  assert (udpgso_limit_segments_by_payload (1200, 0, 65507) == 0);
  return 0;
}
