#!/usr/bin/env python

import sys, os, os.path, shlex, subprocess
from subprocess import Popen as execScript
from distutils.core import setup
from distutils.command.bdist_rpm import bdist_rpm as _bdist_rpm

pkg_name = 'emi-cream-nagios'
pkg_version = '1.1.1'
pkg_release = '3'

source_items = "setup.py src script"

class bdist_rpm(_bdist_rpm):

    def run(self):

        topdir = os.path.join(os.getcwd(), self.bdist_base, 'rpmbuild')
        builddir = os.path.join(topdir, 'BUILD')
        srcdir = os.path.join(topdir, 'SOURCES')
        specdir = os.path.join(topdir, 'SPECS')
        rpmdir = os.path.join(topdir, 'RPMS')
        srpmdir = os.path.join(topdir, 'SRPMS')
        
        cmdline = "mkdir -p %s %s %s %s %s" % (builddir, srcdir, specdir, rpmdir, srpmdir)
        execScript(shlex.split(cmdline)).communicate()
        
        cmdline = "tar -zcf %s %s" % (os.path.join(srcdir, pkg_name + '.tar.gz'), source_items)
        execScript(shlex.split(cmdline)).communicate()
        
        specOut = open(os.path.join(specdir, pkg_name + '.spec'),'w')
        cmdline = "sed -e 's|@PKGVERSION@|%s|g' -e 's|@PKGRELEASE@|%s|g' project/%s.spec.in" % (pkg_version, pkg_release, pkg_name)
        execScript(shlex.split(cmdline), stdout=specOut, stderr=sys.stderr).communicate()
        specOut.close()
        
        cmdline = "rpmbuild -ba --define '_topdir %s' %s.spec" % (topdir, os.path.join(specdir, pkg_name))
        execScript(shlex.split(cmdline)).communicate()


libexec_list = [
                "src/cream_allowedSubmission.py",
                "src/cream_jobCancel.py", 
                "src/cream_jobPurge.py", 
                "src/cream_jobSubmit.py", 
                "src/cream_serviceInfo.py",
                "script/hostname.jdl",
                "script/sleep.jdl"
              ]

libexec_wn_list = [
                    "script/WN-csh.jdl",
                    "script/WN-csh.sh",
                    "script/WN-softver.jdl",
                    "script/WN-softver.sh"
                  ]

setup(
      name=pkg_name,
      version=pkg_version,
      description='Nagios probe for the EMI CREAM and WN services',
      long_description='''This package contains a set of NAGIOS plugins  
used to monitor a CREAM CE node.''',
      license='Apache Software License',
      author_email='CREAM group <cream-support@lists.infn.it>',
      py_modules=['cream'],
      package_dir={'': 'src'},
      data_files=[
                  ('usr/libexec/grid-monitoring/probes/emi.cream', libexec_list),
                  ('usr/libexec/grid-monitoring/probes/emi.cream/wn', libexec_wn_list)
                 ],
      cmdclass={'bdist_rpm': bdist_rpm}
     )


