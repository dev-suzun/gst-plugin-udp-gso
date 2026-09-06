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
  return 0;
}

