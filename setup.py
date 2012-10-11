#!/usr/bin/env python

from distutils.core import setup
import ConfigParser

pkg_version = '0.0.0'
try:
    parser = ConfigParser.ConfigParser()
    parser.read('setup.cfg')
    pkg_version = parser.get('global','pkgversion')
except:
    pass

config_list = [
               "config/emi.nagios-jobmngt.conf"
              ]

libexec_list = [
                "src/CREAMCEDJS-probe",
                "src/CREAMCE-probe"
               ]

setup(
      name='cream-nagios',
      version=pkg_version,
      description='Nagios probe for the EMI CREAM and WN services',
      long_description='''This package contains the following Nagios probes:
CREAMCE-probe, CREAMCEDJS-probe, samtest-run.
- The probes can run in active and/or passives modes (in Nagios sense).
  Publication of passive test results from inside of probes can be done
  via Nagios command file or NSCA.
- On worker nodes Nagios is used as probes scheduler and executer. Metrics
  results from WNs can be sent to Message Broker.''',
      license='Apache Software License',
      author='CREAM group',
      author_email='CREAM group <cream-support@lists.infn.it>',
      packages=['creammetrics'],
      package_dir = {'': 'src'},
      data_files=[
                  ('etc/gridmon', config_list),
                  ('usr/libexec', libexec_list)
                 ]
     )


