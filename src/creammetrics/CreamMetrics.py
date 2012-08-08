# Copyright (c) Members of the EGEE Collaboration. 2004. 
# See http://www.eu-egee.org/partners/ for details on the copyright
# holders.  
#
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 
#
#     http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.

#
# Authors: Konstantin Skaburskas <konstantin.skaburskas@cern.ch>, CERN
#

"""
Nagios CREAM metrics.

Module containing Nagios CREAM metrics.
Multi-threaded version.

Konstantin Skaburskas <konstantin.skaburskas@cern.ch>, CERN
SAM (Service Availability Monitoring)
"""

import os
import sys
import getopt
import time
import commands
import re
import shutil

from gridmon import probe
from gridmon import utils as samutils
from gridmon.nagios.perfdata import PerfData
from creammetrics import sched as sched
from creammetrics.jdl import JDLTemplate

# Python modules WN job depends on
global WNJOB_MODDEPS
WNJOB_MODDEPS = ['gridmon',
                 'creammetrics']

# for threaded metricJobMonit()
gridjobs_states = {}

MINUTE = 60 # 1 min

class CEGenericMetrics(probe.MetricGatherer):
    """A Metric Gatherer generic for CE/CREAMCE."""

    # The probe's author name space
    ns = 'emi.cream'

    _wmses = []

    # Timeouts
    _timeouts = {#'JOB_BABYSITTING' : 30,
                  'WNJOB_GLOBAL' : '600',
                  'JOB_WAITING' : MINUTE*45,
                  'JOB_GLOBAL'  : MINUTE*55,
                  'JOB_DISCARD' : MINUTE*60*6, # 6h
                  'JOB_DISCARD_MONIT_SCHEDRUN' : int(MINUTE*60*5.5) # 5h30min
                  }

    # max number of times job can successively be in Scheduled or Running
    max_count_SchedRun = 2

    # host to be tested
    hostName   = None
    portNumber = None

    # Default is to use Message Broker 
    _no_mb = False

    # Message Broker URI. No defaults.
    # 'failover://(stomp://<hostname>:<port>/,stomp://<hostname>:<port>/)'
    _mb_uri = ''

    # Destination queue/topic on Message Broker
    _mb_destination = ''

    # Brokers network.
    _mb_network = 'PROD'

    # By default do broker discovery on WN.
    _mb_discovery = True

    # What algorithm to use to choose a broker on WN
    _mb_choice = 'best'
    _mb_choices = ['best',   # min response time
                   'random']

    _dirAddWnTarNag = []
    _addWnTarNagSAM = True
    _addWnTarNagSAMcfg = True

    # JDL template file
    _fileJDLTemplateFN = 'CREAM-jdl.template'
    _fileJDLTemplate = '%s/%s' % (
            os.path.normpath(os.path.dirname(os.path.abspath(sys.argv[0]))),
            _fileJDLTemplateFN)

    # JDL ClassAdds
    _jdlRetryCount = '0'
    _jdlShallowRetryCount = '1'

    # WN scheduler
    _dirWnjobLocation = '%s/wnjob' % (
            os.path.normpath(os.path.dirname(os.path.abspath(sys.argv[0]))))

    # Do not submit job to CE
    _nosubmit = None

    _hosts = []

    prev_status = ''

    # Framework verbosity level on WN [0-3]
    _wn_verb_fw = '1'
    # Metrics verbosity level on WN [0-3]
    _wn_verb = '0'

    # Configuration
    _config_file = "/etc/gridmon/emi.nagios-jobmngt.conf"
    _config_param_name = 'config'

    # Probe info
    probeinfo = { 'probeName'      : ns+'.CREAMCE-Probe',
                  'probeVersion'   : '1.0',
                  'serviceVersion' : '*'}
    # Metrics' info
    _metrics = {
           'JobState' : {
                      'metricDescription': "Submit a grid job to CE.",
                      'cmdLineOptions'   : ['namespace=',
                                            'wms=',
                                            'mb-destination=',
                                            'mb-uri=',
                                            'mb-network=',
                                            'mb-no-discovery',
                                            'mb-choice=',
                                            'no-mb',
                                            'timeout-wnjob-global=',
                                            'add-wntar-nag=',
                                            'add-wntar-nag-nosam',
                                            'add-wntar-nag-nosamcfg',
                                            'jdl-templ=',
                                            'jdl-retrycount=',
                                            'jdl-shallowretrycount=',
                                            'wnjob-location=',
                                            'wn-verb=',
                                            'wn-verb-fw=',
                                            '%s=' % _config_param_name,
                                            'timeout-job-discard=',
                                            'no-submit',
                                            'prev-status='],
                      'cmdLineOptionsReq' : [],
                      'metricChildren'   : [],
                      'statusMsgs'       : {
                                    'OK'      :'OK: [Submitted]',
                                    'WARNING' :'WARNING: Problem with job submission to CE.'  ,
                                    'CRITICAL':'CRITICAL: Problem with job submission to CE.' ,
                                    'UNKNOWN' :'UNKNOWN: Problem with job submission to CE.'},
                      'metricVersion'     : '0.1'
                      },

           'JobMonit' : {
                      'metricDescription': "Babysit submitted grid jobs.",
                      'cmdLineOptions'   : ['namespace=',
                                            'timeout-job-global=',
                                            'timeout-job-waiting=',
                                            'hosts=',
                                            '%s=' % _config_param_name,
                                            'timeout-job-discard=',
                                            'timeout-job-schedrun='],
                      'cmdLineOptionsReq' : [],
                      'metricChildren'   : [],
                      'metricVersion'     : '0.1'
                      },
           'JobCancel' : {
                      'metricDescription': "Cancel grid jobs.",
                      'cmdLineOptions'   : ['namespace=',
                                            '%s=' % _config_param_name],
                      'cmdLineOptionsReq' : [],
                      'metricChildren'   : [],
                      'metricVersion'     : '0.1'
                      },
           }

    # <submitTimeStamp>|<hostNameCE>|<serviceDesc>|<jobID>|<jobState>|<lastStateTimeStamp>
    activejobattrs = ['submitTimeStamp',
                      'hostNameCE',
                      'serviceDesc',
                      'jobID',
                      'jobState',
                      'lastStateTimeStamp']

    jobstates = {'all':['SUBMITTED',
                        'WAITING',
                        'READY',
                        'SCHEDULED',
                        'RUNNING',
                        'DONE',
                        'ABORTED',
                        'CLEARED'],
                 'terminal':['DONE',
                             'ABORTED',
                             'CLEARED'],
                 'non_terminal':['SUBMITTED',
                                 'WAITING',
                                 'READY',
                                 'SCHEDULED',
                                 'RUNNING']}

    # TODO: MOVE OUT FROM THE CLASS
    gridjobcmd = {'SUBMIT'  : 'glite-wms-job-submit -a',
                  'STATUS'  : 'glite-wms-job-status',
                  'OUTPUT'  : 'glite-wms-job-output --noint',
                  'CANCEL'  : 'glite-wms-job-cancel --noint',
                  'LOGGING' : 'glite-wms-job-logging-info -v 2'}

    # Order of performance data for JobMonit metric.
    jm_perf_data_order = ('jobs_processed', 'DONE', 'RUNNING', 'SCHEDULED',
                          'SUBMITTED', 'READY', 'WAITING', 'WAITING-CANCELLED',
                          'WAITING-CANCEL', 'ABORTED', 'CANCELLED', 'CLEARED',
                          'MISSED', 'UNDETERMINED', 'unknown')

    def __init__(self, tuples, cetype):

        probe.MetricGatherer.__init__(self, tuples, cetype)

        self.usage="""    Metrics specific parameters:

--namespace <string>    Name-space for the probe. (Default: %s)
--%s <file1,>       Comma separated list of metrics configuration files.
                        (Default: %s)
%s
--mb-destination <dest> Mandatory parameter (if --no-mb is not specified). 
                        The destination queue/topic on Message Broker to publish to.
--mb-uri <URI>     Message Broker URI. If not given, MB discovery will be
                   performed on WN to find working MB.
                   Format for <URI>: [failover://\(]<uri>,[...][\)]
                   <uri> - stomp://FQDN:port/ or http://FQDN/message
                   (Default: service discovery on WN.)
--mb-network <net> Brokers network for broker discovery on WN. (Default: %s)
--mb-no-discovery  Do not do broker discovery on WN. If a given <URI> is not
                   accessible, WN part of the framework will exit with UKNOWN.
                   (Default: if <URI> is not given or not accessible from WN
                   perform broker discovery in <net>.)
--mb-choice <best|random>
                   How to choose MB on WN. 'best' - min response time.
                   (Defalult: %s)
--no-mb          Do not send results messages to Message Broker
--wms <wms>      WMS to be used for job submission. If not given, default
                 WMProxy end-points defined on the UI will be used.
--timeout-wnjob-global <sec>   Global timeout for a job on WN. (Default: %s)
--add-wntar-nag <d1,d2,..>  Comma-separated list of top level directories with
                            Nagios compliant directories structure to be added
                            to tarball to be sent to WN.
--add-wntar-nag-nosam       Instructs the metric not to include standard SAM WN
                            probes and their Nagios config to WN tarball.
                            (Default: WN probes are included)
--add-wntar-nag-nosamcfg    Instructs the metric not to include Nagios
                            configuration for SAM WN probes to WN tarball. The
                            probes themselves and respective Python packages,
                            however, will be included.
--jdl-templ <file>    JDL template file (full path). Default:
                      <emi.cream.ProbesLocation>/%s
--jdl-retrycount <val>         JDL RetryCount (Default: %s).
--jdl-shallowretrycount <val>  JDL ShallowRetryCount (Default: %s).
--wnjob-location <dir>         Full path to directory contaning WN scheduler.
                               (Default: <emi.cream.ProbesLocation>/wnjob)
--wn-verb <0-3>             Metrics verbosity level on WN. [-v <VERBOSITY>]
                            (Default: %s)
--wn-verb-fw <0-3>          Framework verbosity level on WN (Default: %s)
--timeout-job-discard <sec> Discard job after the timeout. (Default: %s)
--prev-status <0-3>         Previous Nagios status of the metric.

%s
--timeout-job-global <sec>  Global timeout for jobs. Job will be canceled
                            and dropped if it is not in terminal state by
                            that time. (Default: %i)
--timeout-job-waiting <sec> Time allowed for a job to stay in Waiting with
                            'no compatible resources'. (Default: %i)
--timeout-job-discard <sec> Discard job after the timeout. (Default: %s)
--timeout-job-schedrun <sec> Scheduled/Running states timeout. (Default: %s)
--hosts <h1,h2,..>          Comma-separated list of CE hostnames to run
                            the monitor on.

"""%(self.ns,
     self._config_param_name,
     self._config_file,
     self.ns+'.'+self.serviceType+'-JobState',
     self._mb_network,
     self._mb_choice,
     self._timeouts['WNJOB_GLOBAL'],
     self._fileJDLTemplateFN,
     self._jdlRetryCount,
     self._jdlShallowRetryCount,
     self._wn_verb,
     self._wn_verb_fw,
     self._timeouts['JOB_DISCARD'],
     self.ns+'.'+self.serviceType+'-JobMonit',
     self._timeouts['JOB_GLOBAL'],
     self._timeouts['JOB_WAITING'],
     self._timeouts['JOB_DISCARD'],
     self._timeouts['JOB_DISCARD_MONIT_SCHEDRUN']
     )

        # read configuration
        self._read_config(tuples)

        # initiate metrics description
        self.set_metrics(self._metrics)

        # parse command line parameters
        self.parse_cmd_args(tuples)

        # working directory for metrics
        self.make_workdir()

        # File to track status of the submitted job. Structure:
        # <submitTimeStamp>|<hostNameCE>|<serviceDesc>|<jobID>|<jobState>|<lastStateTimeStamp>
        self._fileActiveJobName = 'activejob.map'
        self._fileActiveJob = self.workdir_metric+'/'+self._fileActiveJobName

        # Lock file
        self._fileLock = self.workdir_metric+'/lock'

        # Job output parameters
        self._dirJobOutputName = 'jobOutput'
        self._dirJobOutput = self.workdir_metric+'/'+self._dirJobOutputName
        self._fileJobID    = self.workdir_metric+'/jobID'
        self._fileJobOutputName = 'gridjob.out'

        # Nagios launcher and babysitter on WN
        self._fileWNExeName  = 'nagrun.sh'
        self._fileWNExeFPath = self.workdir_metric+'/'+self._fileWNExeName

        # WN test job tarball
        self._fileWNTarballName  = 'gridjob.tgz'
        self._fileWNTarballFPath = self.workdir_metric+'/'+self._fileWNTarballName

        # JDL
        self._fileJDL  = self.workdir_metric+'/gridJob.jdl'
        self._jdlPatterns = {
                            'jdlExecutable': self._fileWNExeName,
                            'jdlArguments': None,
                            'jdlInputSandboxExecutable': self._fileWNExeFPath,
                            'jdlInputSandboxTarball': self._fileWNTarballFPath,
                            'jdlReqCEInfoHostName': None,
                            'jdlRetryCount': self._jdlRetryCount,
                            'jdlShallowRetryCount': self._jdlShallowRetryCount,
                            }

        self.metJobSubmission = '%s.%s-JobSubmit-%s' % (self.ns,
                                                        self.serviceType,
                                                        self.fqan or self.voName)
        self.metJobState = '%s.%s-JobState-%s' % (self.ns, self.serviceType,
                                                  self.fqan or self.voName)

    def parse_args(self, opts):
        """Parse command line arguments relevant to the probe and metrics.
        """
        self.printdvm('Parsing command line parameters.')

        for o,v in opts:
            if o in '--wms':
                for w in v.split(','):
                    self._wmses.append(w)
            elif o == '--mb-destination':
                self._mb_destination = v
            elif o == '--mb-uri':
                self._mb_uri = v
            elif o == '--mb-network':
                self._mb_network = v
            elif o == '--mb-no-discovery':
                self._mb_discovery = False
            elif o == '--mb-choice':
                if v in self._mb_choices:
                    self._mb_choice = v
                else:
                    raise getopt.GetoptError('--mb-choice <%s>, given %s' % \
                                             ('|'.join(self._mb_choices), v))
            elif o == '--no-mb':
                self._no_mb = True
            elif o == '--timeout-wnjob-global':
                self._timeouts['WNJOB_GLOBAL'] = v
            elif o == '--timeout-job-global':
                self._timeouts['JOB_GLOBAL'] = int(v)
            elif o == '--timeout-job-waiting':
                self._timeouts['JOB_WAITING'] = int(v)
            elif o == '--timeout-job-discard':
                self._timeouts['JOB_DISCARD'] = int(v)
            elif o == '--timeout-job-schedrun':
                self._timeouts['JOB_DISCARD_MONIT_SCHEDRUN'] = int(v)
            elif o == '--add-wntar-nag':
                for d in v.split(','):
                    self._dirAddWnTarNag.append(d)
            elif o == '--add-wntar-nag-nosam':
                self._addWnTarNagSAM = False
            elif o == '--add-wntar-nag-nosamcfg':
                self._addWnTarNagSAMcfg = False
            elif o == '--namespace':
                self.ns = v
            elif o == '--jdl-templ':
                self._fileJDLTemplate = v
            elif o == '--jdl-retrycount':
                self._jdlRetryCount = v
            elif o == '--jdl-shallowretrycount':
                self._jdlShallowRetryCount = v
            elif o == '--wnjob-location':
                self._dirWnjobLocation = v
            elif o == '--no-submit':
                self._nosubmit = True
            elif o == '--hosts':
                self._hosts = [ x for x in v.split(',') if x ]
            elif o == '--wn-verb-fw':
                try:
                    self._wn_verb_fw = str(abs(int(v)))
                except ValueError:
                    raise getopt.GetoptError('--wn-verb-fw should be integer.')
            elif o == '--wn-verb':
                try:
                    self._wn_verb = str(abs(int(v)))
                except ValueError:
                    raise getopt.GetoptError('--wn-verb should be integer.')
            elif o == '--'+self._config_param_name:
                self._config_file = [ x for x in v.split(',') if x ]
            elif o == '--prev-status':
                try:
                    self.prev_status = samutils.to_status(abs(int(v)))
                except ValueError:
                    raise getopt.GetoptError('--prev-status should be integer.')

        if re.search('JobState$', self.get_execMetric()) and \
                ( not self._no_mb and not self._mb_destination):
            raise getopt.GetoptError('--mb-destination <dest> is required')

        if not self._mb_discovery and not self._mb_uri and not self._no_mb:
            status = 'UNKNOWN'
            summary_txt = 'No MB URI given and MB discovery is disabled.'
            self.printd(summary_txt)
            self.printd('ATTENTION: Test results cannot be published from WN!')
            self.printd('Bailing out.')
            summary = '%s: %s\n' % (status, summary_txt)
            sys.stdout.write(summary)
            sys.stdout.write('%s%s' % (summary, self.get_detdata()))
            sys.exit(self.retCodes[status])

    def _read_config(self, tuples):
        '''Read configuration file(s).
        '''
        self.printdvm('Reading configuration file(s).')

        cfile = self.get_optarg_from_Tuples(tuples, self._config_param_name)
        if cfile:
            self._config_file = [ x for x in cfile.split(',') if x ]

        self.printd('Configuration files:\n %s' % \
                    '\n '.join(self._config_file), 1)

        import ConfigParser
        config = ConfigParser.ConfigParser()
        fs = config.read(self._config_file)
        if fs:
            self.printd('Reading from configuration file(s):\n %s' % \
                        '\n '.join(fs), 1)
            if config.has_section('ce_metrics'):
                try:
                    self._timeouts['JOB_WAITING'] = MINUTE*config.getint('ce_metrics',
                                                                  'timeout_job_waiting')
                except ConfigParser.NoOptionError:
                    pass
                except ValueError,e:
                    raise "Bad value for 'timeout_job_waiting'", str(e)
                try:
                    self._timeouts['JOB_GLOBAL'] = MINUTE*config.getint('ce_metrics',
                                                                  'timeout_job_global')
                except ConfigParser.NoOptionError:
                    pass
                except ValueError,e:
                    raise "Bad value for 'timeout_job_global'", str(e)
                try:
                    self._timeouts['JOB_DISCARD'] = MINUTE*config.getint('ce_metrics',
                                                                  'timeout_job_discard')
                except ConfigParser.NoOptionError:
                    pass
                except ValueError,e:
                    raise "Bad value for 'timeout_job_discard'", str(e)
                try:
                    self._timeouts['JOB_DISCARD_MONIT_SCHEDRUN'] = \
                                            MINUTE*config.getint('ce_metrics',
                                                        'timeout_job_schedrun')
                except ConfigParser.NoOptionError:
                    pass
                except ValueError,e:
                    raise "Bad value for 'timeout_job_schedrun'", str(e)
                try:
                    self._jdlRetryCount = str(config.getint('ce_metrics',
                                                    'jdl_RetryCount'))
                except ConfigParser.NoOptionError:
                    pass
                except ValueError,e:
                    raise "Bad value for 'jdl_RetryCount'", str(e)
                try:
                    self._jdlShallowRetryCount = str(config.getint('ce_metrics',
                                                    'jdl_ShallowRetryCount'))
                except ConfigParser.NoOptionError:
                    pass
                except ValueError,e:
                    raise "Bad value for 'jdl_ShallowRetryCount'", str(e)
                for k in self.gridjobcmd.keys():
                    try:
                        self.gridjobcmd[k] = config.get('ce_metrics',
                                                        'JOB_'+k)
                    except ConfigParser.NoOptionError:
                        pass
                    except ValueError,e:
                        raise "Bad value for 'JOB_%s'"%k, str(e)
                try:
                    self._wn_verb = str(config.getint('ce_metrics',
                                                      'wn_verb'))
                except ConfigParser.NoOptionError:
                    pass
                except ValueError,e:
                    raise "Bad value for 'wn_verb'", str(e)
                try:
                    self._wn_verb_fw = str(config.getint('ce_metrics',
                                                         'wn_verb_fw'))
                except ConfigParser.NoOptionError:
                    pass
                except ValueError,e:
                    raise "Bad value for 'wn_verb_fw'", str(e)
        else:
            self.printd('None of configuration files read:\n %s' % \
                            '\n '.join(self._config_file), 1)

    def metricJobState(self):
        """Submit a grid job to CE.
        """

        # Clean up or exit gracefully if there is an active job.
        try:
            os.stat(self._fileActiveJob)
        except OSError:
            # Active job file was not found. Do cleanup.
            self._js_do_cleanup()
        else:
            rc, jobdesc = self._load_activejob()
            if not rc:
                status = 'UNKNOWN'
                s = 'Problems loading active job file.'
                self.printd('%s\n%s' % (s, jobdesc))
                self.prints(s)
                return status

            subtime     = int(jobdesc['submitTimeStamp'])
            jobsts      = jobdesc['jobState']

            # active job was found
            self.printdvm('>>> Active job found')
            self.printdvm('file: %s' % self._fileActiveJob)
            self.printdvm('\n'.join([ k+' : '+v for k,v in jobdesc.items()]))

            if jobsts.upper() not in self.jobstates['terminal']:
                #if lastststime - subtime > self._timeouts['JOB_DISCARD']:
                if int(time.time()) - subtime >= self._timeouts['JOB_DISCARD']:
                    cmd = self.gridjobcmd['CANCEL']+' '+jobdesc['jobID']
                    self.printdvm('>>> Old job - cancel it\n%s' % cmd)
                    _rc, _stsmsg, _detmsg = self.run_cmd(cmd)
                    os.unlink(self._fileActiveJob)
                else:
                    status = self.prev_status or 'OK'
                    s = 'Active job - %s [%s]' % \
                        (jobsts, time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime(subtime)))
                    self.printd(jobdesc['jobID'])
                    self.prints(s)
                    return status
            else:
                self.printdvm('>>> Deleting active job file.')
                self.printdvm('The previously submitted job is in terminal state: %s\n' % \
                            jobdesc['jobState'])
                self.printd('Deleting the file %s' % self._fileActiveJob, v=3)
                try:
                    os.unlink(self._fileActiveJob)
                except StandardError: pass
                self._js_do_cleanup()

        # TODO: global timeout on checking job's status is needed. 30 min?

        self.printdvm('>>> Preparing for job submission')

        # job's output directory
        if not os.path.isdir(self._dirJobOutput):
            try:
                os.makedirs(self._dirJobOutput)
            except OSError, e:
                status = 'UNKNOWN'
                summary = "OSError: %s" % str(e)
                self.printd(summary)
                return (status, summary)

        wmsendpt = ''
        if len(self._wmses) != 0:
            wms, port = samutils.parse_uri(self._wmses[0])
            if not port:
                port = '7443'
            wmsendpt = '-e https://%s:%s/glite_wms_wmproxy_server'%(wms, port)

        self.printdvm('>>> WMS to be used')
        self.printdvm(wmsendpt != '' and wmsendpt or 'System default.')

        if os.environ.has_key('PROBES_HOME'):
            probeshome = os.environ['PROBES_HOME']
        else:
            probeshome = samutils.get_launchdir()

        # build archive that will go to WN
        rc, s, d = self._js_package_wnjob(probeshome)
        if samutils.to_status(rc) != 'OK':
            self.printd(d)
            self.prints(s)
            return rc

        # prepare the script which will launch Nagios on WN
        fpgridexe = self._dirWnjobLocation+'/'+self._fileWNExeName
        if not os.path.exists(fpgridexe):
            status = 'UNKNOWN'
            msg = "Path [%s] doesn't exist, while creating JDL." % \
                        (fpgridexe)
            self.printd(msg)
            return (status, msg)
        shutil.copy(fpgridexe, self._fileWNExeFPath)

        # Build and save JDL
        self.printdvm('>>> Building JDL:')
        self._js_build_jdl_patternsubst()
        jdl = JDLTemplate(self._fileJDLTemplate, self._fileJDL,
                                                self._jdlPatterns)
        jdl.build_save()

        # Check if jobs ID file is there. If, yes:
        # - cancel all the jobs defined there
        # - delete the file
        if os.path.exists(self._fileJobID):
            cmd = self.gridjobcmd['CANCEL']+' -i %s' % self._fileJobID
            # TODO: use this returned info later for more verbose output
            _rc, _stsmsg, _detmsg = self.run_cmd(cmd)
            os.unlink(self._fileJobID)

        # submit job
        cmd = self.gridjobcmd['SUBMIT']+' %(wms)s -o %(jobids)s %(jdl)s' % \
            {'wms':wmsendpt, 'jobids':self._fileJobID, 'jdl':self._fileJDL}
        self.printdvm('>>> Job submit command\n%s\n' % cmd)

        if self._nosubmit:
            self.printdvm('>>> Asked not to submit job. Bailing out ... ', cr=False)
            #self._js_do_cleanup()
            self.printd('Asked not to submit job. Bailing out')
            self.prints('Asked not to submit job. Bailing out')
            return 'OK'

        self.printdvm('>>> Submmitting grid job ... ', cr=False)
        rc, summary, detmsg = self.run_cmd(cmd, _verbosity=2)

        if rc != self.retCodes['OK']:
            self.printdvm('failed.')
            self._js_do_cleanup()
            self.printd(detmsg)
            self.prints(summary)
            return 'UNKNOWN'
        self.printdvm('done.')

        self.printd(detmsg)

        # obtain job's ID
        try:
            jobid = re.search('^http.*$', detmsg, re.M).group(0).rstrip()
            self.printdvm('>>> Job ID\n%s' % jobid)
        except (IndexError, AttributeError):
            status = 'UNKNOWN'
            s = "Job was successfully submitted, "+\
                "but we couldn't determine the job's ID."
            self.printd(s)
            self.prints(s)
            return status

        # save active job's description for jobs' monitor
        stime = str(int(time.time()))
        jd = {}
        jd['submitTimeStamp'] = stime
        jd['hostNameCE']      = self.hostName
        jd['serviceDesc']     = self.metJobSubmission
        jd['jobID']           = jobid
        jd['jobState']       = 'Submitted'
        jd['lastStateTimeStamp'] = stime
        self._save_activejob(jd)

        self.prints(summary)

        return rc

    def metricJobCancel(self):
        """Cancel grid job and remove job bookkeeping files.
        """
        _cmd = self.gridjobcmd['CANCEL']+' %s'
        # job ID can be either in jobID or active job map files
        if os.path.exists(self._fileJobID):
            cmd = _cmd % (' -i %s' % self._fileJobID)
        elif os.path.exists(self._fileActiveJob):
            rc, jobdesc = self._load_activejob()
            if not rc:
                status = 'UNKNOWN'
                s = 'Problems loading active job file.'
                self.printd('%s\n%s' % (s, jobdesc))
                self.prints(s)
                return status
            cmd = _cmd % jobdesc['jobID']
        else:
            msg = 'UNKNOWN: no active job found'
            return 'UNKNOWN', msg, msg
        _rc, _stsmsg, _detmsg = self.run_cmd(cmd)
        self.printd('Job cancellation request sent:\n%s' % cmd)
        self.printdvm(_detmsg)
        for f in [self._fileJobID, self._fileActiveJob]:
            try: os.unlink(f)
            except Exception: pass
        self.printd('Job bookkeeping files deleted.')
        return 'OK', 'OK: job cancelled'

    def metricJobMonit(self):
        """Babysitter for grid jobs.
        """
        perf_data = PerfData(self.jm_perf_data_order)

        def __get_badjds():
            if not badjds:
                return ''
            return '\n%s\nBad job descriptions:\n%s' % \
                    ('-'*21, '---\n'.join(badjds))

        def __status():
            if badjds:
                return 'WARNING'
            else:
                return 'OK'

        def __update_perfdata_from_jobstates(perfdata, js):
            perfdata['unknown'] = [0,1,2]
            for k,v in js.items():
                k = re.sub(' .*$', '', k)
                k = re.sub('[][><?!@]', '', k).upper()
                if k in self.jm_perf_data_order:
                    if perfdata.has_key(k):
                        perfdata[k] += v
                    else:
                        perfdata[k] = v
                else:
                    perfdata['unknown'][0] += v
            return perfdata

        # get active job files
        hostsdir = self.workdir_service
        if self._hosts:
            hosts = self._hosts
        else:
            hosts = os.listdir(hostsdir)
        hostsajd = {}
        badjds = []
        for h in hosts:
            ajf = '%s/%s/%s' % (hostsdir, h, self._fileActiveJobName)
            try:
                os.stat(ajf)
            except OSError:
                pass
            else:
                rc,jd = self._load_activejob(ajf)
                if rc:
                    hostsajd[h] = jd
                else:
                    badjds.append('%s - %s' % (h,jd))

        # no active jobs
        if not hostsajd:
            status = __status()
            summary = detmsg = status+': no active jobs ['+\
                time.strftime("%Y-%m-%dT%H:%M:%SZ")+']'
            self.perf_data = [('jobs_processed', 0)]
            return (status, summary, detmsg + __get_badjds())

        self.printdvm('>>> Active jobs to process')
        self.printdvm('\n'.join([x['jobID'] for x in hostsajd.values()]))

        # spawn threads to process active jobs
        thrsched = sched.SchedulerThread()
        for h,jd in hostsajd.items():
            thrsched.run(self._thr_ajworker, jd, h)

        thrsched.wait()

        status = __status()
        summary = detmsg = status+': Jobs processed - '+str(len(hostsajd))

        pd = {'jobs_processed' : len(hostsajd)}

        global gridjobs_states
        js = {}
        for h in hostsajd.keys():
            try:
                try:
                    s = gridjobs_states[h]
                    if re.match('Failure', s):
                        status = 'UNKNOWN'
                except KeyError:
                    s = 'Missed [%s]' % h
                js[s] += 1
            except KeyError:
                js[s] = 1
        jss = '\n'.join([ k+' : '+str(v) for k,v in js.items() ])
        detmsg += '\n'+jss
        # performance data
        perf_data.update(__update_perfdata_from_jobstates(pd, js))
        self.perf_data = perf_data
        return (status, summary, detmsg + __get_badjds())

    def _thr_ajworker(self, jd):
        """Check status of submitted grid job and udate the grid
        *job status* (CREAMCE-JobSubmit) and/or *CREAMCE state* (CREAMCE-JobState).
        (The "update" is perfomed in the sense of Nagios passive
        checks.)

        - jd - job description
        """
        # NB! only one status transition per job state is allowed
        states_counters = [{'states'     : ('Scheduled', 'Running'),
                            'max_count'  : self.max_count_SchedRun,
                            'from_status': 'WARNING',
                            'to_status'  : 'CRITICAL'}]
        summary = ''
        try:
            detmsg = '\n%s' % self.details_header
        except AttributeError:
            detmsg = ''

        def __update_non_terminal(state, jd, ajf, _detmsg=''):
            """Update active job file and job status passive check.
            """
            jd['jobState'] = state
            jd['lastStateTimeStamp'] = str(int(time.time()))
            s = self._save_activejob(jd, ajf)
            self.printdvm('>>> %s\nUpdated active job file\n%s\n' % (host, s))

            # job status
            status = 'OK'
            summary = detmsg = status+': ['+state+'] '+jd['jobID']
            detmsg += _detmsg or ''
            self._submit_results(host, self.metJobState, str(self.retCodes[status]),
                           summary, detmsg)

        def __job_cancelTimeout(timeout, status, update_JobSubmit=False):
            # cancel the job
            _detmsg = detmsg+\
                        "\n\n%i min timeout for the job exceeded. Cancelling the job." % \
                        (timeout/60)
            rc, detmsgC = __job_cancel(jobid)
            if rc != self.retCodes['OK']:
                jobsts = '[%s->Cancel failed [timeout/dropped]]' % jobstate
                _summsg = jobsts+" "+summary+" Problem cancelling job."
                _detmsg = jobsts+" "+_detmsg+" Problem cancelling job.\n"+detmsgC
            else:
                jobsts = '[%s->Cancelled [timeout/dropped]]' % jobstate
                _summsg = jobsts+" "+summary
                _detmsg = jobsts+" "+_detmsg+' '+" "+detmsgC

            _, msg = __job_logging_info(jobid)
            _detmsg += msg

            # status update depending on status counters for particular states
            status, count_str = self._status_from_states_counter(host,
                                                                 jobstate,
                                                                 status,
                                                                 states_counters)
            if count_str:
                _summsg = '%s %s' % (count_str, _summsg)
                _detmsg = '%s %s' % (count_str, _detmsg)

            # Job Status
            self._submit_results(host, self.metJobState, str(self.retCodes[status]),
                                 '%s: %s' % (status, _summsg),
                                 '%s: %s' % (status, _detmsg))
            # Job Submit
            if update_JobSubmit:
                self._submit_results(host, srvdesc, str(self.retCodes[status]),
                                     '%s: %s' % (status, _summsg),
                                     '%s: %s' % (status, _detmsg))
            os.unlink(ajf)
            gridjobs_states[host] = jobsts
            sched.de_queue(host)
            return (status, _summsg, _detmsg)

        def __removeStatusStanza(str):
            return re.sub('(%s).*\n' % '|'.join(samutils.retCodes.keys()),
                          '\n', str, 1)

        def __failedGetJobStatus(detmsg):
            gridjobs_states[host] = 'Failure [get job status] %s: %s' % (host,detmsg)
            if re.search('command not found', detmsg, re.M):
                # JobState
                status = 'WARNING'
                summary = 'unable to get job status. Command not found.'
                detmsg = '%s\n%s' % (summary, detmsg)
                self._submit_results(host, self.metJobState, str(self.retCodes[status]),
                               status+': '+summary, status+': '+detmsg)
            else:
                _, _detmsg = __job_logging_info(jobid)
                detmsg += '\n\nLogging info:\n%s' % _detmsg
                if int(time.time()) - subtime >= self._timeouts['JOB_DISCARD']:
                    # JobState
                    status = 'WARNING'
                    summary = 'unable to get job status. Job discarded.'
                    detmsg = '%s\n%s' % (summary, detmsg)
                    self._submit_results(host, self.metJobState, str(self.retCodes[status]),
                                   status+': '+summary, status+': '+detmsg)
                    # JobSubmit
                    status = 'UNKNOWN'
                    self._submit_results(host, self.metJobSubmission, str(self.retCodes[status]),
                                   status+': '+summary, status+': '+detmsg)
                    # remove the job
                    shutil.move(ajf, ajf+'.FAILURE_GETSTATUS')
                else:
                    # JobState
                    status = 'WARNING'
                    summary = 'unable to get job status. Job will be deleted in %i min' % \
                                ((self._timeouts['JOB_DISCARD'] - (int(time.time()) - subtime)) / 60)
                    detmsg = '%s\n%s' % (summary, detmsg)
                    self._submit_results(host, self.metJobState, str(self.retCodes[status]),
                                   status+': '+summary, status+': '+detmsg)
            sched.de_queue(host)
            return (rc, summary, summary+detmsg)

        def __undefinedJobStatus():
            status = 'WARNING'
            summary = status+': unable to determine job status.'
            # CE-JobState
            self._submit_results(host, self.metJobState, str(rc),
                                 summary,
                                 '%s\nJob id: %s' % (summary,jobid))
            status = 'UNKNOWN'
            summary = status+': unable to determine job status.'
            # CE-JobSubmit
            self._submit_results(host, self.metJobSubmission, str(rc),
                                 summary,
                                 '%s\nJob id: %s' % (summary,jobid))

            shutil.move(ajf, ajf+'.FAILURE_UNDEFSTATUS')
            gridjobs_states[host] = 'Failure [undentified job status]'
            sched.de_queue(host)
            return (rc, summary, detmsg)

        def __run_cmd(action, jobid):
            cmd = '%s %s' % (self.gridjobcmd[action],jobid)
            rc, _, msg = self.thr_run_cmd(cmd, _verbosity=2)
            return rc, __removeStatusStanza(msg)

        def __job_logging_info(jobid):
            return __run_cmd('LOGGING', jobid)

        def __job_cancel(jobid):
            return __run_cmd('CANCEL', jobid)

        def __job_status(jobid):
            return __run_cmd('STATUS', jobid)

        subtime = int(jd['submitTimeStamp'])
        host    = jd['hostNameCE']
        srvdesc = jd['serviceDesc']
        jobid   = jd['jobID']
        jobsts  = jd['jobState']
        # FIXME: unused
        #lastststime = int(jd['lastStateTimeStamp'])

        workdir = '%s/%s' % (self.workdir_service, host)
        ajf = workdir+'/'+self._fileActiveJobName

        self.printdvm('>>> %s: Active job attributes' % host)
        self.printdvm('\n'.join([ k+' : '+v for k,v in jd.items()]))

        global gridjobs_states

        rc, detmsgS = __job_status(jobid)
        detmsg += detmsgS
        if rc != self.retCodes['OK']:
            return __failedGetJobStatus(detmsgS)

        # parse details output and determine the job's status
        match = re.search('^Current Status:.*$', detmsgS, re.M)
        if not match:
            return __undefinedJobStatus()
        currstat = match.group(0).rstrip()
        self.printdvm('>>> %s: %s %s' % (host,
                            time.strftime("%Y-%m-%dT%H:%M:%SZ"),currstat))

        # TODO: check times "lastStateTimeStamp - submitTimeStamp" for jobs not in terminal states
        # if job is older than JOB_OLD and not in terminal state - discard the job

        #non_terminal_re = '('+'|'.join([ s.title() for s in jobstates['non_terminal'] ])+')'
        timeout_JOB_DISCARD = self._timeouts['JOB_DISCARD']
        timeout_JOB_DISCARD_MONIT_SCHEDRUN = \
                                self._timeouts['JOB_DISCARD_MONIT_SCHEDRUN']
        timeout_JOB_GLOBAL = self._timeouts['JOB_GLOBAL']
        currtime = int(time.time())

        if re.search('Done.*\((Success|Exit Code.*)\)', currstat):
            jobstate = 'Done'

            djo = workdir+'/'+self._dirJobOutputName

            self.printdvm('>>> %s:\n' % host+\
                          'Job is in Done state.\n'+\
                          'Get results and submit passive check result to '+\
                          '<%s,%s>\n' % (host, srvdesc))

            detmsg += '\nGetting job output: '
            cmd = self.gridjobcmd['OUTPUT']+' --dir %s %s 2>&1' % (djo, jobid)
            rc, summary, _detmsg = self.thr_run_cmd(cmd, _verbosity=2)
            if samutils.to_status(rc) != 'OK':
                detmsg += 'Failed.\n'
                detmsg += __removeStatusStanza(_detmsg)
                summary += ' Failed to get job output.'
            else:
                detmsg += 'OK.\n'
            summary = re.sub('(%s): ' % '|'.join(samutils.retCodes.keys()),
                          '', summary, 1)
            status = samutils.to_status(rc)

            if re.search('Done.*Exit Code.*', currstat) or self.verbosity >= 3:
                def __job_output_file():
                    cmd = 'find %s -name %s -cmin -1' % (djo,
                                                         self._fileJobOutputName)
                    _, o = commands.getstatusoutput(cmd)
                    if o:
                        return o.split('\n')[0]
                    else:
                        import getpass
                        return '%s/%s_%s/%s' % (djo, getpass.getuser(),
                                                jobid[jobid.rfind('/')+1:],
                                                self._fileJobOutputName)
                def __get_retcode_wn(detmsgS):
                    'Get return code out of WN exit code.'
                    re_exit_prep = '^Exit code:( )*'
                    match = re.search(re_exit_prep+'[0-9]+( )*$', detmsgS, re.M)
                    if match:
                        wn_rc = re.sub(re_exit_prep, '', match.group(0).rstrip(), 1)
                        try:
                            return samutils.to_retcode(int(wn_rc))
                        except ValueError:
                            raise ValueError, 'Error getting WN exit code: exit code is not integer.'
                    else:
                        raise ValueError, 'Error getting WN exit code: no exit code found.'

                try:
                    status = samutils.to_status(__get_retcode_wn(detmsgS))
                except ValueError, e:
                    detmsg += '\n%s\n' % str(e)
                else:
                    summary = 'job submission OK - problem on WN'
                fjo = __job_output_file()
                detmsg += '\n\nJob output:\n'
                try:
                    detmsg = '%s\n%s' % (detmsg, open(fjo,'r').read())
                except IOError, e:
                    detmsg = '%s\n%s' % (detmsg,
                        'Could not open job output file: %s\n%s' % (fjo, str(e)))
                summary += ' [Done (Exit Code !=0)]'

            # status update depending on status counters for particular job states
            status, count_str = self._status_from_states_counter(host,
                                                                 jobstate,
                                                                 status,
                                                                 states_counters)
            if count_str:
                summary = '%s %s' % (count_str, summary)
            summary = '%s: %s' % (status, summary)

            # Job State
            self._submit_results(host, srvdesc, str(samutils.to_retcode(status)),
                                 summary, '%s\n%s' % (summary, detmsg))
            # Job Submit
            self._submit_results(host, self.metJobState, str(samutils.to_retcode(status)),
                                 summary, '%s\n%s' % (summary, detmsg))

            os.unlink(ajf)
            gridjobs_states[host] = jobstate
            sched.de_queue(host)
            return (status, summary, detmsg)

        elif re.search('Aborted', currstat):
            jobstate = 'Aborted'
            def __job_aborted_reason(msg):
                if re.search('Reason.*request expired', msg, re.M):
                    if re.search('Reason.*BrokerHelper: no compatible resources',
                                 msg, re.M):
                        status = 'CRITICAL'
                        summary = 'Job was aborted. Failed to match.'
                    else:
                        status = 'UNKNOWN'
                        summary = 'Job was aborted. Check WMS.'
                else:
                    status = 'CRITICAL'
                    summary = 'Job was aborted.'
                return status, summary

            rc, msg = __job_logging_info(jobid)
            if samutils.to_status(rc) != 'OK':
                status = 'UNKNOWN'
                summary = "Job was aborted. Failed to get job logging info."
            else:
                status, summary = __job_aborted_reason(msg)
            detmsg += msg

            # status update depending on status counters for particular states
            status, count_str = self._status_from_states_counter(host,
                                                                 jobstate,
                                                                 status,
                                                                 states_counters)
            if count_str:
                summary = '%s %s' % (count_str, summary)
            summary = '%s: %s' % (status, summary)

            # Job Submit
            self._submit_results(host, srvdesc, str(self.retCodes[status]),
                                 summary, '%s\n%s' % (summary, detmsg))
            # Job State
            self._submit_results(host, self.metJobState, str(self.retCodes[status]),
                                 summary, '%s\n%s' % (summary, detmsg))

            os.unlink(ajf)
            gridjobs_states[host] = 'Aborted'
            sched.de_queue(host)
            return (status, summary, detmsg)

        elif re.search('Waiting', currstat):
            jobstate = 'Waiting'
            timeout_JOB_WAITING = self._timeouts['JOB_WAITING']
            def __logging_info_job(jobid):
                rc, detmsgL = __job_logging_info(jobid)
                if samutils.to_status(rc) != 'OK':
                    detmsgL = '%s\n%s' % ('Failed to get job logging info.',
                                          detmsgL)
                return detmsgL
            def __cancel_job(jobid):
                rc, detmsgC = __job_cancel(jobid)
                if rc != self.retCodes['OK']:
                    jobsts = '[%s->Cancel failed [timeout/dropped]]' % jobstate
                    _summsg = '%s %s Problem cancelling job. %s' % (jobsts, summary, jobid)
                    _detmsg = '%s Problem cancelling job.\n%s' % (detmsg, detmsgC)
                else:
                    jobsts = '[%s->Cancelled [timeout/dropped]]' % jobstate
                    _summsg = '%s %s %s' % (jobsts, summary, jobid)
                    _detmsg = '%s %s' % (detmsg, detmsgC)
                return jobsts, _summsg, _detmsg

            match = re.search('^Status Reason:.*$', detmsg, re.M)
            if match:
                # "Status Reason:      BrokerHelper: no compatible resources"
                if re.search("no compatible resources", match.group(0).rstrip()):
                    summary = "'BrokerHelper: no compatible resources'."
                    if currtime - subtime >= timeout_JOB_WAITING:
                        status = 'CRITICAL'
                        detmsg += "\n%i min timeout for status [%s] exceeded. Cancelling the job." % \
                                    ((timeout_JOB_WAITING/60), jobstate)
                        detmsgL = __logging_info_job(jobid)
                        jobsts, summary, detmsg = __cancel_job(jobid)
                        detmsg += '\n%s\n%s' % ('JOB STATUS:\n%s' % detmsgS,
                                                'JOB LOGGING INFO:\n%s' % detmsgL)

                        # status update depending on status counters for particular states
                        status, count_str = self._status_from_states_counter(host,
                                                                             jobstate,
                                                                             status,
                                                                             states_counters)
                        if count_str:
                            summary = '%s %s' % (count_str, summary)
                        summary = '%s: %s' % (status, summary)

                        # Job Submit
                        self._submit_results(host, srvdesc, str(self.retCodes[status]),
                                             summary, '%s\n%s' % (summary, detmsg))
                        os.unlink(ajf)
                    else:
                        # update active job file
                        status = 'WARNING'
                        jobsts = '[%s]' % jobstate
                        jd['jobState'] = jobstate
                        jd['lastStateTimeStamp'] = str(int(time.time()))
                        s = self._save_activejob(jd, ajf)
                        self.printdvm('>>> %s:\nUpdated active job file\n%s\n' % (host,s))

                        summary = '%s: [%s] %s' % (status, jobstate, summary)
                        detmsg = '%i min (timeout: %i min)\n%s' % \
                                    ((int(time.time()) - subtime)/60,
                                     timeout_JOB_WAITING/60, detmsg)

                    # Job State
                    self._submit_results(host, self.metJobState, str(self.retCodes[status]),
                                   summary, '%s\n%s' % (summary, detmsg))

                    gridjobs_states[host] = jobsts
                    sched.de_queue(host)
                    return (status, summary, detmsg)
                else:
                    # unrecognized reason for status Waiting
                    if currtime - subtime >= timeout_JOB_DISCARD:
                        return __job_cancelTimeout(timeout_JOB_DISCARD, 'UNKNOWN',
                                                   update_JobSubmit=True)
            else:
                summary = 'No Status Reason given.'
                if currtime - subtime >= timeout_JOB_WAITING:
                    status = 'UNKNOWN'
                    detmsg += "%i min timeout for status [%s] exceeded. Cancelling the job." % \
                                ((timeout_JOB_WAITING/60), jobstate)
                    detmsgL = __logging_info_job(jobid)
                    jobsts, summary, detmsg = __cancel_job(jobid)
                    detmsg += '\n%s\n%s' % ('JOB STATUS:\n%s' % detmsgS,
                                            detmsgL)
                    # status update depending on status counters for particular states
                    status, count_str = self._status_from_states_counter(host,
                                                                         jobstate,
                                                                         status,
                                                                         states_counters)
                    if count_str:
                        summary = '%s %s' % (count_str, summary)
                    summary = '%s: %s' % (status, summary)

                    # Job Submit
                    self._submit_results(host, srvdesc, str(self.retCodes[status]),
                                         summary, '%s\n%s' % (summary, detmsg))
                    os.unlink(ajf)
                else:
                    jobsts = '[%s]' % jobstate
                    jd['jobState'] = jobstate
                    jd['lastStateTimeStamp'] = str(int(time.time()))
                    s = self._save_activejob(jd, ajf)
                    self.printdvm('>>> %s:\nUpdated active job file\n%s\n' % (host,s))

                    status = 'OK'
                    summary = '%s: [%s] %s %s' % (status, jobstate, jobid)
                    detmsg = detmsgS

                # Job State
                self._submit_results(host, self.metJobState, str(self.retCodes[status]),
                                     summary, '%s\n%s' % (summary, detmsg))

                gridjobs_states[host] = jobsts
                sched.de_queue(host)
                return (status, summary, detmsg)

        elif re.search('Submitted', currstat):
            jobstate = 'Submitted'
            if currtime - subtime >= timeout_JOB_GLOBAL:
                return __job_cancelTimeout(timeout_JOB_GLOBAL, 'UNKNOWN',
                                           update_JobSubmit=True)
            __update_non_terminal(jobstate, jd, ajf, _detmsg=detmsg)
            jobsts = '[%s]'%jobstate

        elif re.search('Ready', currstat):
            jobstate = 'Ready'
            if currtime - subtime >= timeout_JOB_GLOBAL:
                return __job_cancelTimeout(timeout_JOB_GLOBAL, 'UNKNOWN',
                                           update_JobSubmit=True)
            __update_non_terminal(jobstate, jd, ajf, _detmsg=detmsg)
            jobsts = '[%s]'%jobstate

        elif re.search('Scheduled', currstat):
            jobstate = 'Scheduled'
            if currtime - subtime >= timeout_JOB_DISCARD_MONIT_SCHEDRUN:
                return __job_cancelTimeout(timeout_JOB_DISCARD_MONIT_SCHEDRUN,
                                           'WARNING',
                                           update_JobSubmit=True)
            __update_non_terminal(jobstate, jd, ajf, _detmsg=detmsg)
            jobsts = '[%s]'%jobstate

        elif re.search('Running', currstat):
            jobstate = 'Running'
            if currtime - subtime >= timeout_JOB_DISCARD_MONIT_SCHEDRUN:
                return __job_cancelTimeout(timeout_JOB_DISCARD_MONIT_SCHEDRUN,
                                           'WARNING',
                                           update_JobSubmit=True)
            __update_non_terminal(jobstate, jd, ajf, _detmsg=detmsg)
            jobsts = '[%s]'%jobstate

        elif re.search('Cleared', currstat):
            # Job was cleared, but active job file was left.
            # There is not need to update job's status.
            jobstate = 'Cleared'
            status = 'UNKNOWN'
            self.printdvm('>>> %s: Warning. Job was cleared, ' % host +\
                          'but active job file was left\n%s' % ajf)
            os.unlink(ajf)
            jobsts = 'Cleared'

        elif re.search('Cancelled', currstat):
            # Job was cancelled, but active job file was left. Seems like it wasn't
            # us who did cancele the job.
            #==============================================================
            # *************************************************************
            # BOOKKEEPING INFORMATION:
            #
            # Status info for the Job : https://wms206.cern.ch:9000/OoiKF7G
            # Current Status:     Cancelled
            # Logged Reason(s):
            #    -
            #    - Aborted by user
            # Status Reason:      Aborted by user.
            # Destination:        agh2.atlas.unimelb.edu.au:2119/jobmanager
            # Submitted:          Tue Jan 27 10:33:23 2009 CET
            # *************************************************************
            #==============================================================
            jobstate = 'Cancelled'
            status = 'UNKNOWN'

            self.printdvm('>>> %s: Warning. Job was cancelled, ' % host +\
                          'but active job file was left\n%s' % ajf)
            os.unlink(ajf)
            jobsts = 'Cancelled'

        else:
            jobstate = 'UNDETERMINED'
            self.printd('>>> %s: Warning. Job is in %s: %s' % (host, jobstate,
                                                               jobid))
            if currtime - subtime >= timeout_JOB_GLOBAL:
                return __job_cancelTimeout(timeout_JOB_GLOBAL, 'UNKNOWN',
                                           update_JobSubmit=False)
            __update_non_terminal(jobstate, jd, ajf, _detmsg=detmsg)
            jobsts = '[%s]'%jobstate

        gridjobs_states[host] = jobsts
        sched.de_queue(host)
        return(rc, summary, detmsg)

    def _load_activejob(self, ajf=''):
        """Loads active job's description from formated file.
        """

        if not ajf:
            ajf = self._fileActiveJob

        self.printdvm("Loading active job's description ... ",
              cr=False)

        sched.mutex.acquire()
        vals = open(ajf).readline().strip().split('|')
        sched.mutex.release()
        if len(vals) == 1 and vals[0] == '':
            vals = []
        # we don't want neither less nor empty fields
        if len(self.activejobattrs) != len(vals):
            self.printdvm('failed.')
            output = 'ERROR: File format mismatch while reading '+ajf+'.\n'
            output += 'File should contain '+str(len(self.activejobattrs))+\
                ' elements, but '+str(len(vals))+' read.\n'
            return False, output
        elif [ True for v in vals if v == '' ]:
            self.printdvm('failed.')
            output = 'ERROR: File format mismatch while reading '+ajf+'.\n'
            output += 'One of the required fields is empty.\n'
            return False, output

        self.printdvm('done.')

        return True, dict(zip(self.activejobattrs, vals))

    def _save_activejob(self, data, ajf=''):
        """Saves active job's description to a file.
        - data - dictionary with self.activejobattrs fields
        """

        if not ajf:
            ajf = self._fileActiveJob

        self.printdvm("Saving active job's description ... ",
              cr=False)

        s = '|'.join([ data[k] for k in self.activejobattrs ])

        sched.mutex.acquire()
        open(ajf,'w').write(s)
        sched.mutex.release()

        self.printdvm('done.')

        return s

    def _js_do_cleanup(self, clean=True):
        if not clean:
            return
        for fn in [self._fileJobID,
                   self._fileJDL,
                   self._fileWNExeFPath,
                   self._fileWNTarballFPath]:
            try:    os.unlink(fn)
            except StandardError: pass
        # keep last job's output sandbox
        try:
            if os.listdir(self._dirJobOutput):
                new_dir = self._dirJobOutput+'.LAST'
                shutil.rmtree(new_dir, ignore_errors=True)
                os.rename(self._dirJobOutput, new_dir)
        except StandardError: pass

    def _js_package_wnjob(self, probeshome):
        """ Build an archive which will go to WN.
        """

        global WNJOB_MODDEPS

        # check if working directory with Nagios exists
        dirnagname = 'nagios.d'
        dirnag = probeshome+'/wnjob/'+dirnagname
        if not os.path.exists(dirnag):
            status = 'UNKNOWN'
            s = "Path [%s] doesn't exist, while building grid job tarball." % \
                        dirnag
            return (status, s, s)

        if self._dirAddWnTarNag and \
            [ False for d in self._dirAddWnTarNag if not os.path.exists(d) ]:
            status = 'UNKNOWN'
            s = "Path [%s] doesn't exist, while adding to grid job tarball." % d
            return (status, s, s)

        # Create zipped tarball with Nagios, probes, library etc. to be used on WNs.
        self.printdvm('>>> Creating WN tarball')
        self.printdvm(self._fileWNTarballFPath)

        # Use system's 'tar' utility.
        # Python's tarfile module is too slow.

        # copy Nagios to the probe's working directory
        worknag = self.workdir_metric+'/'+dirnagname
        if os.path.exists(worknag):
            shutil.rmtree(worknag, ignore_errors=True)
        shutil.copytree(dirnag, worknag)

        # dir where Nagios grid probes should be stored
        nagprobes = worknag+'/probes'
        if not os.path.exists(nagprobes):
            try:    os.mkdir(nagprobes)
            except StandardError: pass

        # add EMI WN probes to WN tarball
        if self._addWnTarNagSAM:
            if self._addWnTarNagSAMcfg:
                # take org.sam probes and Nagios configuration
                cmd = 'cp -rf %s/* %s' % (probeshome+'/wnjob/emi.wn',
                                          worknag)
                self.printdvm('>>> Adding emi.wn probes and Nagios config:')
            else:
                # drop org.sam Nagios configuration, take only probes
                cmd = 'cp -rf %s/* %s' % (probeshome+'/wnjob/emi.wn/probes',
                                          worknag+'/probes')
                self.printdvm('>>> Adding emi.wn probes only:')
            self.printdvm(cmd)
            try:
                s, o = commands.getstatusoutput(cmd)
            except StandardError, e:
                status = 'UNKNOWN'
                summsg = 'Unable to copy SAM directory structure '+\
                            'while building WN tarball.'
                detmsg = '%s\n%s\n%s' % (summsg, cmd, str(e))
                return (status, summsg, detmsg)
            else:
                if s != 0:
                    status = 'UNKNOWN'
                    summsg = 'Unable to copy SAM directory structure '+\
                                'while building WN tarball.'
                    detmsg = '%s\n%s\n%s' % (summsg, cmd, o)
                    return (status, summsg, detmsg)

            # copy required modules
            self.printdvm('>>> Adding SAM Python modules:')
            self.printdvm(', '.join(WNJOB_MODDEPS))
            for mn in WNJOB_MODDEPS:
                if not os.environ.has_key('PROBES_TOPLIB'):
                    m = __import__(mn)
                    mp = m.__path__[0]
                else:
                    mp = os.environ['PROBES_TOPLIB']+'/'+mn
                shutil.copytree(mp, nagprobes+'/'+mn)

            # copy errors DB to to "nagprobes" directory
            self.printdvm('>>> Adding Errors DB:\n%s' % self.errorDBFile)
            if os.environ.has_key('PROBES_ERRDB'):
                fperrdb = os.environ['PROBES_ERRDB']
            else:
                fperrdb = self.errorDBFile
            shutil.copyfile(fperrdb, nagprobes+'/gridmon.errdb')

        # add externally provided directories
        if self._dirAddWnTarNag:
            cmd = 'cp -rf %s/* %s' % ('/* '.join(self._dirAddWnTarNag), worknag)
            self.printdvm('>>> Adding externally provided directories:')
            self.printdvm(', '.join(self._dirAddWnTarNag))
            self.printdvm(cmd)
            try:
                s, o = commands.getstatusoutput(cmd)
            except StandardError, e:
                status = 'UNKNOWN'
                summsg = 'Unable to copy provided directory '+\
                            'structure while building WN tarball.'
                detmsg = '%s\n%s\n%s' % (summsg, cmd, str(e))
                return (status, summsg, detmsg)
            else:
                if s != 0:
                    status = 'UNKNOWN'
                    summsg = 'Unable to copy provided directory '+\
                                'structure while building WN tarball.'
                    detmsg = '%s\n%s\n%s' % (summsg, cmd, o)
                    return (status, summsg, detmsg)

        # create Nagios archive
        self.printdvm('>>> Creating the archive ... ', cr=False)
        os.chdir(worknag)
        samutils.arch_zip(self._fileWNTarballFPath, ['.'],
                          exclude=['*.pyc','*.pyo','.svn','*~'])
        self.printdvm('done.')

        os.chdir(self.workdir_metric)
        try:    shutil.rmtree(worknag, ignore_errors=True) #os.removedirs(dir)
        except StandardError: pass

        # move to probes working directory
        os.chdir(self.workdir_metric)

        return ('OK', '', '')

    def _js_build_jdl_patternsubst(self):
        '''Builds dictionary of pattern substitutions in JDL.
        '''

        self._jdlPatterns['jdlArguments'] = "-v %(vo)s %(voms)s -t %(wnjobtimeout)s -w %(verb)s " % {
            'vo'             : self.voName,
            'voms'           : self.fqan and "-f %s" % self.fqan or ' ',
            'wnjobtimeout'   : self._timeouts['WNJOB_GLOBAL'],
            'verb'           : self._wn_verb_fw}

        # If we don't want to use message broker 
        if self._no_mb:
            self._jdlPatterns['jdlArguments'] = '%s -m' % ( self._jdlPatterns['jdlArguments'] )
        else: 

            # MB URI may come in the following format: proto://(...,...).
            # We need to escape "()" for the shell on WN.
            if self._mb_uri:
                _mb_uri = self._mb_uri.replace('(', '\(')
                _mb_uri = "-b '" + _mb_uri.replace(')', '\)') + "'"
            else:
                _mb_uri = ''

            if self._mb_choice == 'random':
                _mb_choice = '-R'
            else:
                _mb_choice = ''

            self._jdlPatterns['jdlArguments'] = "%(old)s -d %(mb_destination)s %(mb_uri)s %(mb_discovery)s -n %(mb_network)s %(mb_choice)s " % {
                'old'            : self._jdlPatterns['jdlArguments'],
                'mb_destination' : self._mb_destination,
                'mb_uri'         : _mb_uri,
                'mb_discovery'   : not self._mb_discovery and '-B' or '',
                'mb_network'     : self._mb_network,
                'mb_choice'      : _mb_choice}

        self._jdlPatterns['jdlReqCEInfoHostName'] = self.hostName

    def _submit_results(self, host, mname, rc, summary, detmsg):
        """Submit check results.
        """
        metric_res = [{'host'    : host,
                       'service' : mname,
                       'status'  : str(rc),
                       'summary' : summary,
                       'details' : detmsg}]
        self._submit_service_checks(metric_res)

    def _states_counter(self, host, jobstate, status, states, max_count,
                        from_status, to_status):
        '''Keeps track of a number of consecutive states of a metric. Uses
        a file to store the counter.
        '''
        fn = '%s/%s/STATES_COUNTER_%s' % (self.workdir_service, host, '-'.join(states))
        count = 0
        if jobstate in states and status == from_status:
            sched.mutex.acquire()
            if os.path.exists(fn):
                fp = open(fn, 'r')
                count = int(fp.read().strip())
                fp.close()
                if count + 1 >= max_count:
                    status = to_status
                count += 1
            else:
                count = 1
            open(fn, 'w').write(str(count))
            sched.mutex.release()
        else:
            self._unlink(fn)
        return (status, count)

    def _status_from_states_counter(self, host, jobstate, status, states_counters):
        '''Returns a tuple: (state, counter string)
        '''
        counter_str = ''
        for sc in states_counters:
            to_status, counter = self._states_counter(host,
                                        jobstate, status,
                                        sc['states'], sc['max_count'],
                                        sc['from_status'], sc['to_status'])
            # status change occured
            if status != to_status and counter:
                counter_str = '[%i%s/%i]' % (counter, status[0],
                                             sc['max_count'])
                status = to_status
                break
            # status change didn't occur, but counter was reset
            elif status == to_status and counter:
                counter_str = '[%i/%i]' % (counter, sc['max_count'])
                break
        return status, counter_str

    def _unlink(self, fn):
        try:
            os.unlink(fn)
        except StandardError:
            pass


