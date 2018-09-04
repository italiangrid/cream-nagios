#!/usr/bin/env python

import sys, os, os.path, shlex, subprocess
from subprocess import Popen as execScript
from distutils.core import setup
from distutils.command.bdist_rpm import bdist_rpm as _bdist_rpm

pkg_name = 'nagios-plugins-cream.cream-service'
pkg_version = '1.2.1'
pkg_release = '1'

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


python_scripts = [
                  "src/cream_allowedSubmission.py",
                  "src/cream_jobCancel.py", 
                  "src/cream_jobPurge.py", 
                  "src/cream_jobSubmit.py", 
                  "src/cream_serviceInfo.py",
                 ]

bash_scripts = [
                "script/WN-softver.sh",
                "script/WN-csh.sh"
               ]

etc_list = [
            "script/hostname.jdl",
            "script/sleep.jdl",
            "script/WN-softver.jdl,
            "script/WN-csh.jdl"
           ]

setup(
      name=pkg_name,
      version=pkg_version,
      description='Nagios probe for the EMI CREAM and WN services',
      long_description='''This package contains a set of NAGIOS plugins  
used to monitor a CREAM CE node.''',
      license='Apache Software License',
      author_email='CREAM group <cream-support@lists.infn.it>',
      packages=['it.infn.monitoring'],
      package_dir={'': 'src'},
      scripts=python_scripts,
      data_files=[
                  ('usr/libexec/argo-monitoring/probes/it.infn.monitoring', bash_scripts),
                  ('etc/nagios/plugins/it.infn.monitoring', etc_list)
                 ],
      cmdclass={'bdist_rpm': bdist_rpm}
     )


