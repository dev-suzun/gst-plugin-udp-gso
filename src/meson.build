# SPDX-License-Identifier: BSD-3-Clause

udpgso_sources = files(
  'gstudpgsoplugin.c',
  'gstudpgsosink.c',
  'udpgsobatch.c',
  'udpgsoschedule.c',
)

gst_plugins_dir = gst_dep.get_variable(pkgconfig: 'pluginsdir')

shared_library(
  'gstudpgso',
  udpgso_sources,
  dependencies: [gst_dep, gst_base_dep],
  c_args: ['-D_GNU_SOURCE'],
  install: true,
  install_dir: gst_plugins_dir,
)

