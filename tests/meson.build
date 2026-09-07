# SPDX-License-Identifier: BSD-3-Clause

batch_test = executable(
  'test-batch-layout',
  ['test_batch_layout.c', '../src/udpgsobatch.c'],
  include_directories: include_directories('../src'),
)
test('batch-layout', batch_test)

schedule_test = executable(
  'test-schedule',
  ['test_schedule.c', '../src/udpgsoschedule.c'],
  include_directories: include_directories('../src'),
)
test('schedule', schedule_test)

kernel_gso_test = executable('test-kernel-gso', 'test_kernel_gso.c')
test('kernel-gso', kernel_gso_test, timeout: 10)

kernel_txtime_test = executable('test-kernel-txtime', 'test_kernel_txtime.c')
test('kernel-txtime', kernel_txtime_test, timeout: 10)

sink_loopback_test = executable(
  'test-sink-loopback',
  [
    'test_sink_loopback.c',
    '../src/gstudpgsosink.c',
    '../src/udpgsobatch.c',
    '../src/udpgsoschedule.c',
  ],
  dependencies: [gst_dep, gst_base_dep, gst_app_dep],
  include_directories: include_directories('../src'),
  c_args: ['-D_GNU_SOURCE'],
)
test('sink-loopback', sink_loopback_test, timeout: 10)

