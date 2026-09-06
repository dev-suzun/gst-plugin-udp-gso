batch_test = executable(
  'test-batch-layout',
  ['test_batch_layout.c', '../src/udpgsobatch.c'],
  include_directories: include_directories('../src'),
)
test('batch-layout', batch_test)

kernel_gso_test = executable('test-kernel-gso', 'test_kernel_gso.c')
test('kernel-gso', kernel_gso_test, timeout: 10)

