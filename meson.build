project(
  'gst-plugin-udp-gso',
  'c',
  version: '0.1.0',
  license: 'BSD-3-Clause',
  meson_version: '>= 0.61',
  default_options: ['c_std=gnu11', 'warning_level=3'],
)

cc = meson.get_compiler('c')

gst_dep = dependency('gstreamer-1.0', version: '>= 1.20')
gst_base_dep = dependency('gstreamer-base-1.0', version: '>= 1.20')
gtk_dep = dependency(
  'gtk+-3.0',
  version: '>= 3.22',
  required: get_option('demo'),
)

if host_machine.system() != 'linux'
  error('gst-plugin-udp-gso currently requires Linux')
endif

if not cc.has_function(
  'sendmmsg',
  prefix: '#define _GNU_SOURCE\n#include <sys/socket.h>',
)
  error('sendmmsg() is required')
endif

subdir('src')
subdir('tests')

if gtk_dep.found()
  subdir('demo')
else
  message('GTK 3 development files not found; udpgso-demo will not be built')
endif

