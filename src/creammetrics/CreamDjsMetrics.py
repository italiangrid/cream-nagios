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
metrics for testing direct job submission to CREAM CE.

Metrics for testing direct job submission to CREAM CE.

Konstantin Skaburskas <konstantin.skaburskas@cern.ch>, CERN
SAM (Service Availability Monitoring)
"""

import os
import sys
import time
import signal
import commands
import re
import shutil
import getopt

import httplib
import urllib2
from urllib2 import URLError
import logging
import suds
from suds.transport.http import HttpTransport
from suds.client import Client

from gridmon import probe
from gridmon import utils as samutils
from gridmon import gridutils
from gridmon import metricoutput
from gridmon.nagios.perfdata import PerfData
from gridmon.errmatch import *
from creammetrics import sched as sched

# for threaded metricJobMonit()
gridjobs_states = {}

httplib.HTTPSConnection.debuglevel = 0

class HttpClientAuthTransport(HttpTransport):
    def __init__(self, key, cert):
        HttpTransport.__init__(self)
        self.urlopener = urllib2.build_opener(HTTPSClientAuthHandler(key, cert))

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    def __init__(self, key, cert):
        urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert
    def https_open(self, req):
        return self.do_open(self.getConnection, req)
    def getConnection(self, host):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

class CREAMCEDirectSubmissonMetrics(probe.MetricGatherer):
    """A Metric Gatherer generic for CREAMCE with direct job submission."""

    # The probe's author name space
    ns = 'emi.cream'

    # host to be tested
    hostName   = None
    portNumber = None

    # resource endnpoint to submit job
    resource = None

    prev_status = ''

    # BDII for resource enpoint descovery
    _ldap_url  = "ldap://sam-bdii.cern.ch:2170"

    # TODO: MOVE OUT FROM THE CLASS
    # Timeouts
    _timeouts = {'ldap_timelimit'   : 10,
                   #'JOB_BABYSITTING' : 30,
                  'WNJOB_GLOBAL' : '600',


                  'JOB_CANCEL_REQUEST' : 15,
                  'JOB_PURGE_REQUEST' : 15,

                  'JOB_GLOBAL'  : 60*60,

                  'JOB_REGISTERED' : 60*2,
                  'JOB_PENDING'    : 60*5,
                  'JOB_IDLE'       : 60*45,
                  'JOB_RUNNING'    : 60*50,
                  'JOB_REALLY-RUNNING' : 60*55,
                  'JOB_HELD'       : 60*59,
                  }

    # Probe info
    probeinfo = { 'probeName'      : ns+'.CE-Probe',
                  'probeVersion'   : '1.0',
                  'serviceVersion' : '*'}
    # Metrics' info
    _metrics = {
                'ServiceInfo': {
                      'metricDescription': "Get CREAM CE service info.",
                      'cmdLineOptions'   : ['namespace=',
                                            ],
                      'cmdLineOptionsReq' : [],
                      'metricChildren'   : [],
                      'statusMsgs'       : {
                                    'OK'      :'OK: [Service info obtained.]',
                                    'WARNING' :'Problem with getting service info.',
                                    'CRITICAL':'Problem with getting service info.',
                                    'UNKNOWN' :'Problem with getting service info.'},
                      'metricVersion'     : '0.1',
                                    },
                'DelegateProxy': {
                      'metricDescription': "Delegate proxy to CREAM CE.",
                      'cmdLineOptions'   : ['namespace=',
                                            ],
                      'cmdLineOptionsReq' : [],
                      'metricChildren'   : [],
                      'statusMsgs'       : {
                                    'OK'      :'OK: [Delegated]',
                                    'WARNING' :'Problem with proxy delegation to CE.',
                                    'CRITICAL':'Problem with proxy delegation to CE.',
                                    'UNKNOWN' :'Problem with proxy delegation to CE.'},
                      'metricVersion'     : '0.1',
                                    },
                'SubmitAllowed': {
                      'metricDescription': "Check if submission to the CREAM CE is allowed.",
                      'cmdLineOptions'   : ['namespace=',
                                            ],
                      'cmdLineOptionsReq' : [],
                      'metricChildren'   : [],
                      'statusMsgs'       : {
                                    'OK'      :'OK: [Submission Allowed]',
                                    'WARNING' :'',
                                    'CRITICAL':'Failue.',
                                    'UNKNOWN' :''},
                      'metricVersion'     : '0.1',
                                    },
                'DirectJobState': {
                      'metricDescription': "Direct job submission to CREAM CE.",
                      'cmdLineOptions'   : ['namespace=',
                                            'resource=',
                                            'ldap-uri=',
                                            'no-submit',
                                            'prev-status='],
                      'cmdLineOptionsReq' : [],
                      'metricChildren'   : [],
                      'statusMsgs'       : {
                                    #'OK'      :'OK: ',
                                    'WARNING' :'',
                                    'CRITICAL':'Job submission failed.',
                                    'UNKNOWN' :'Job submission failed.'},
                      'metricVersion'     : '0.1',
                                    },
                'DirectJobMonit': {
                      'metricDescription': "Babysit submitted grid jobs.",
                      'cmdLineOptions'   : ['namespace=',
                                            'timeout-job-global=',
                                            'timeout-job-waiting='],
                      'cmdLineOptionsReq' : [],
                      'metricChildren'   : [],
                      'statusMsgs'       : {
                                    'OK'      :'',
                                    'WARNING' :'',
                                    'CRITICAL':'',
                                    'UNKNOWN' :''},
                      'metricVersion'     : '0.1',
                                    },
                'DirectJobCancel': {
                      'metricDescription': "Cancel job.",
                      'cmdLineOptions'   : ['namespace=',],
                      'cmdLineOptionsReq' : [],
                      'metricChildren'   : [],
                      'statusMsgs'       : {
                                    'OK'      :'',
                                    'WARNING' :'',
                                    'CRITICAL':'',
                                    'UNKNOWN' :''},
                      'metricVersion'     : '0.1',
                                    },
                }

    # <submitTimeStamp>|<hostNameCE>|<serviceDesc>|<jobID>|<jobState>|<lastStateTimeStamp>
    activejobattrs = ['submitTimeStamp',
                      'hostNameCE',
                      'serviceDesc',
                      'jobID',
                      'jobState',
                      'lastStateTimeStamp']

    # TODO: MOVE OUT FROM THE CLASS
    jobstates = {'all':['REGISTERED', #+ the job has been registered but it has
                                      # not been started yet.
                        'PENDING', #+ the job has been started, but it has still
                                   # to be submitted to the LRMS abstraction
                                   # layer.
                        'IDLE', #+ the job is idle in the LRMS.
                        'RUNNING', #+ the job wrapper, which "encompasses" the
                                   # user job, is running in the LRMS.
                        'REALLY-RUNNING', #+ the actual user job (the one
                                          # specified as Executable in the job
                                          # JDL) is running in the LRMS.
                        'HELD', #+ the job is held (suspended) in the LRMS.
                        'CANCELLED', #+ the job has been cancelled.
                        'ABORTED', #+ errors occurred during the "management" of
                                   # the job, e.g. the submission to the LRMS
                                   # abstraction layer software (BLAH) failed.
                        'DONE-OK', #+ the job has successfully been executed.
                        'DONE-FAILED', #+ the job has been executed, but some
                                       # errors occurred.
                        'UNKNOWN' # the job is an unknown status.
                        ],
                 'terminal':['ABORTED',
                             'CANCELLED',
                             'DONE-OK',
                             'DONE-FAILED'
                             ],
                 'non_terminal':['REGISTERED',
                                 'PENDING',
                                 'IDLE',
                                 'RUNNING',
                                 'REALLY-RUNNING',
                                 'HELD'],
                'can_cancel' : ['PENDING',
                                'IDLE',
                                'RUNNING',
                                'REALLY-RUNNING',
                                'HELD'],
                'can_purge'  : ['REGISTERED',
                                'DONE-OK',
                                'DONE-FAILED',
                                'ABORTED',
                                'CANCELLED']
                }

    # TODO: MOVE OUT FROM THE CLASS
    gridjobcmd = {'SUBMIT'  : 'glite-ce-job-submit -a',
                  'STATUS'  : 'glite-ce-job-status',
                  'CANCEL'  : 'glite-ce-job-cancel --noint',
                  'LOGGING' : 'glite-ce-job-status -L',
                  'PURGE'   : 'glite-ce-job-purge --noint',
                  'DELEGATE' : 'glite-ce-delegate-proxy',
                  'OUTPUT'  : 'glite-ce-job-output --noint',
                  'SUBMITALLOWED' : 'glite-ce-allowed-submission'
                  }

    # Order of performance data for JobMonit metric.
    jm_perf_data_order = ('jobs_processed', 'DONE',
                          'REALLY-RUNNING', 'RUNNING', 'REGISTERED',
                          'PENDING', 'IDLE', 'HELD', 'CANCELLED', 'ABORTED',
                          'UNKNOWN', 'MISSED', 'UNDETERMINED', 'unknown')

    # JDL Template file
    _fileJDLTemplateFN = 'CREAMDJS-jdl.template'
    _fileJDLTemplate = '%s/%s' % (
            os.path.normpath(os.path.dirname(os.path.abspath(sys.argv[0]))),
            _fileJDLTemplateFN)

    # JDL ClassAdds
    _jdlRetryCount = '0'
    _jdlShallowRetryCount = '1'

    # Do not submit job to CE
    _nosubmit = None

    _hosts = []

    # URIs of CREAM CE and CREAM CE Monitor Web Services
    ce_cream_uri = None
    ce_monitor_uri = None

    def __init__(self, tuples, cetype):
        '''
        '''
        probe.MetricGatherer.__init__(self, tuples, cetype)

        self.usage="""    Metrics specific parameters:

--namespace <string>    Name-space for the probe. (Default: %s)

%s
--resource <URI>       CREAM CE to send job to. Format :
                       <host>[:<port>]/cream-<lrms-system-name>-<queue-name>
                       If not given - resource discovery will be performed.
--ldap-uri <URI>       Format [ldap://]hostname[:port[/]]
                       (Default: %s)
--prev-status <0-3>    Previous Nagios status of the metric.

""" % (self.ns,
       self.ns+'.'+self.serviceType+'-DirectJobState',
       self._ldap_url)

        # initiate metrics description
        self.set_metrics(self._metrics)

        # parse command line parameters
        self.parse_cmd_args(tuples)

        # working directory for metrics
        self.make_workdir()

        # define CREAM Web Services URIs
        if not self.portNumber:
            self.portNumber = '8443'
        self.ce_cream_uri = 'https://%s:%s/ce-cream/services/CREAM2?wsdl' % \
                                        (self.hostName, str(self.portNumber))
        self.ce_monitor_uri = 'https://%s:%s/ce-monitor/services/CEMonitor?wsdl'% \
                                        (self.hostName, str(self.portNumber))

        # File to track status of the submitted job. Structure:
        # <submitTimeStamp>|<hostNameCE>|<serviceDesc>|<jobID>|<jobState>|<lastStateTimeStamp>
        self._fileActiveJobName = 'activejob.map'
        self._fileActiveJob = self.workdir_metric+'/'+self._fileActiveJobName

        # Job output parameters
        self._dirJobOutputName = 'jobOutput'
        self._dirJobOutput = self.workdir_metric+'/'+self._dirJobOutputName
        self._fileJobID    = self.workdir_metric+'/jobID'
        self._fileJobOutputName = 'cream.out'

        # WN test job tarball
        self._fileWNTarballName  = 'gridjob.tgz'
        self._fileWNTarballFPath = self.workdir_metric+'/'+self._fileWNTarballName

        # JDL
        self._fileJDL  = self.workdir_metric+'/gridJob.jdl'
        self._jdlPatterns = {
                            'jdlExecutable': '/bin/hostname',
                            'jdlArguments': ' ',
                            #'jdlExecutable': '/bin/sleep',
                            #'jdlArguments': '60',
                            #'jdlInputSandboxExecutable': self._fileWNExeFPath,
                            #'jdlInputSandboxTarball': self._fileWNTarballFPath,
                            }
        self.metJobSubmission = '%s.%s-DirectJobSubmit-%s' % (self.ns,
                                                            self.serviceType,
                                                    self.fqan or self.voName)
        self.metJobState = '%s.%s-DirectJobState-%s' % (self.ns, self.serviceType,
                                                        self.fqan or self.voName)
        # user's proxy
        self.user_key = self.user_cert = os.environ['X509_USER_PROXY']

        # logging in SUDS
        logging.getLogger('suds.resolver').setLevel(logging.CRITICAL)

    def parse_args(self, opts):
        """Parse command line arguments relevant to the probe and metrics.
        """
        for o,v in opts:
            if o == '--namespace':
                self.ns = v
            elif o == '--resource':
                self.resource = v
            elif o == '--ldap-uri':
                [host, port] = samutils.parse_uri(v)
                if port == None or port == '':
                    port = '2170'
                self._ldap_url = 'ldap://'+host+':'+port
                os.environ['LCG_GFAL_INFOSYS'] = host+':'+port
            elif o == '--no-submit':
                self._nosubmit = True
            elif o == '--prev-status':
                try:
                    self.prev_status = samutils.to_status(abs(int(v)))
                except ValueError:
                    raise getopt.GetoptError('--prev-status should be integer.')

    def __getHttpClient(self, url, key, cert):
        transport = HttpClientAuthTransport(key, cert)
        return Client(url, transport = transport)

    def __match_error(self, e):
        ''
        metricSuff = self.execMetric2MetricSuff()
        em = ErrorsMatching(self.errorDBFile, self.errorTopics)
        er = em.match(str(e))
        if er:
            status = er[0][2]
            try:
                summary = self.metrics[metricSuff]['statusMsgs'][status] + ' [ErrDB:' + str(er) + ']'
            except KeyError:
                summary = ''
        else:
            status = 'CRITICAL'
            try:
                summary = self.metrics[metricSuff]['statusMsgs'][status]
            except KeyError:
                summary = ''
        return status, summary

    def __run_ws_method(self, client, meth, input={}, out=''):
        '''Run a given method on a web service

        client - service proxy
        meth   - method to invoke (string)
        input  - dictionary of input parameters
        out    - object type returned by web server
        '''

        try:
            si_type = client.factory.create(out)
        except suds.TypeNotFound, e:
            raise TypeError('suds.TypeNotFound: '+str(e))
        si_attrs = []
        for a in dir(si_type):
            if not a.startswith('_'):
                si_attrs.append(a)
        try:
            if type(input) == dict and input:
                si = getattr(client.service, meth)(**input)
            else:
                si = getattr(client.service, meth)()
        except suds.WebFault, e:
            raise UnboundLocalError('suds.WebFault: ', str(e))
        detmsg = ''
        if si:
            for a in si_attrs:
                try:
                    v = getattr(si, a)
                    detmsg += '%s = %s\n' % (a, str(v))
                except AttributeError: pass
        else:
            detmsg = 'Empty set returned.'
        return detmsg

    def metricServiceInfo(self):
        'Get CREAM CE service info.'
        try:
            client = self.__getHttpClient(self.ce_cream_uri,
                                          self.user_key, self.user_cert)
        except URLError, e:
            status, summary = self.__match_error(str(e))
            self.printd(summary+'\n'+str(e))
            self.prints(summary)
            return status
        except StandardError:
            raise
        else:
            detmsg = self.__run_ws_method(client,
                                          'getServiceInfo',
                                          {'ServiceInfoRequest':0},
                                          'ns0:ServiceInfo')
            summary = 'success'
            self.printd('%s\n%s' % (summary, detmsg))
            self.prints(summary)
            return 'OK'

    def metricDelegateProxy(self):
        'Delegate proxy to CREAM CE.'
        cmd = '%s -e %s %s' % (self.gridjobcmd['DELEGATE'],
                               '%s:%s' % (self.hostName, self.portNumber),
                               '%s-%s' % (self.voName, samutils.uuidstr(5)))
        return self.thr_run_cmd(cmd, _verbosity=2)

    def metricSubmitAllowed(self):
        'Check if submission to the CREAM CE is allowed.'
        verb = ''
        if self.verbosity >= probe.VERBOSITY_MAX:
            verb = '-d'
        cmd = '%s %s %s' % (self.gridjobcmd['SUBMITALLOWED'], verb,
                       '%s:%s' % (self.hostName, self.portNumber))
        return self.thr_run_cmd(cmd, _verbosity=2)

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

    def _build_jdl(self):
        """
        Check if all JDL substitution patterns in the respective
        dictionary are set. Load JDL template file. Make required
        substitutions. Save JDL.
        """

        self.printdvm('>>> JDL patterns for template substitutions\n%s' %
            '\n'.join(['%s : %s'%(k,v) for k,v in self._jdlPatterns.items()]))

        patterns = {}
        for pattern,repl in self._jdlPatterns.items():
            if not repl:
                raise TypeError(
                        'JDL pattern <%s> not initialised while building JDL.' %
                        pattern)
            patterns['<%s>' % pattern] = repl

        try:
            _jdl_templ = open(self._fileJDLTemplate, 'r').read()
        except IOError, e:
            raise IOError('Unable to load JDL template %s. %s' %
                                        (self._fileJDLTemplate, str(e)))
        self.printdvm('>>> JDL template: \nfile: %s\n%s' %
                            (self._fileJDLTemplate, _jdl_templ))

        # do the needful
        for pattern,repl in patterns.items():
            _jdl_templ = re.sub(pattern, repl, _jdl_templ)

        self.printdvm('>>> Resulting JDL:\n%s' % _jdl_templ)

        self.printdvm('>>> Saving JDL to %s ... ' % self._fileJDL, cr=False)
        try:
            open(self._fileJDL, 'w').write(_jdl_templ)
        except IOError, e:
            raise IOError('Unable to save JDL %s. %s' %
                                        (self._fileJDL, str(e)))
        self.printdvm('done.')

    def _aj2text(self, jobdesc):
        'Return textual representation of job.'
        ajattrs = []
        for k,v in jobdesc.items():
            if k.endswith('TimeStamp'):
                try:
                    ajattrs.append('%s : %s [%s]' % (k,v,
                            time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                  time.gmtime(int(v)))))
                except StandardError:
                    ajattrs.append('%s : %s' % (k,v))
            else:
                ajattrs.append('%s : %s' % (k,v))
        return '\n'.join(ajattrs)

    class __ErrCLITimeout(Exception):
        ''
    def __sig_alrm_cli_timeout(self, sig, stack):
        raise self.__ErrCLITimeout
    def __job_operation(self, cmd, timeout=None):
        '''Call 'cmd' with 'timeout' (if defined)
        cmd     - command
        timeout - timeout for SIGALRM in seconds
        '''
        sigset = False
        if timeout and (type(timeout) == int):
            signal.signal(signal.SIGALRM, self.__sig_alrm_cli_timeout)
            signal.alarm(timeout)
            sigset = True
        try:
            _, o = commands.getstatusoutput(cmd)
        except StandardError, e:
            self.printdvm(str(e))
        except self.__ErrCLITimeout:
            self.printdvm('Job operation request timed out after %i sec.' %
                          timeout)
        else:
            self.printdvm(o)
            if sigset:
                signal.alarm(0)
    def _can_cancel(self, jobstate):
        '''Check if the state of the job allows it to be cancelled.
        jobstate - state of the job on CREAM CE
        '''
        try:
            return jobstate in self.jobstates['can_cancel']
        except StandardError:
            return False
    def _can_purge(self, jobstate):
        '''Check if the state of the job allows it to be purged.
        jobstate - state of the job on CREAM CE
        '''
        try:
            return jobstate in self.jobstates['can_purge']
        except StandardError:
            return False
    def _cancel_job(self, jobid, timeout=True):
        ''''Cancel the job on CREAM CE.
        jobid    - id of the job
        timeout  - time out or not (Default: True)
        '''
        cmd = '%s %s' % (self.gridjobcmd['CANCEL'], jobid)
        self.printdvm('Cancel the job\n%s' % cmd)
        if timeout:
            timeout = self._timeouts['JOB_CANCEL_REQUEST']
        try:
            self.__job_operation(cmd, timeout=timeout)
        except Exception, e:
            self.printd('Unhandled exeption while cancelling job: %s' % str(e))
    def _getoutput(self, jobid, outdir):
        ''''Retrieve job output from CREAM CE.
        jobid    - id of the job
        outdir   - where to store job output files
        '''
        cmd = '%s --dir %s %s' % (self.gridjobcmd['OUTPUT'], outdir, jobid)
        self.printdvm('Retrieve the output of the job\n%s' % cmd)
        try:
            self.__job_operation(cmd, timeout=False)
        except Exception, e:
            self.printd('Unhandled exeption while retrieving job output: %s' % str(e))

    def _purge_job(self, jobid, timeout=True):
        ''''Purge the job from CREAM CE.
        jobid    - id of the job
        timeout  - time out or not (Default: True)
        '''
        cmd = '%s %s' % (self.gridjobcmd['PURGE'], jobid)
        self.printdvm('Purge the job\n%s' % cmd)
        if timeout:
            timeout = self._timeouts['JOB_PURGE_REQUEST']
        try:
            self.__job_operation(cmd, timeout=timeout)
        except Exception, e:
            self.printd('Unhandled exeption while purging job: %s' % str(e))
    def _cancelpurge_job(self, jobstate, jobid, timeout=True):
        '''Cancel and purge the job.
        jobstate - state of the job on CREAM CE
        jobid    - id of the job
        timeout  - time out or not (Default: True)
        '''
        if self._can_cancel(jobstate):
            self._cancel_job(jobid, timeout=timeout)
        if self._can_purge(jobstate):
            self._purge_job(jobid, timeout=timeout)
    def _discard_job(self, jobstate, jobid, ajf, timeout=True):
        '''Cancel, purge the job from CREAM CE and delte the job's
        monitoring info from UI.
        jobstate - state of the job on CREAM CE
        jobid    - id of the job
        ajf      - file with the job monitoring info
        timeout  - timeout or not (Default: True)
        '''
        self._cancelpurge_job(jobstate, jobid, timeout=timeout)
        self._unlink(ajf)
    def _unlink(self, fn):
        try:
            os.unlink(fn)
        except StandardError:
            pass

    def _logging_info(self, jobid):
        cmd = '%s %s' % (self.gridjobcmd['LOGGING'], jobid)
        rc, _, msg = self.thr_run_cmd(cmd, _verbosity=2)
        return (rc, msg)


    def metricDirectJobState(self):
        'Submit job to CREAM CE.'

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
            lastststime = int(jobdesc['lastStateTimeStamp'])

            # active job was found
            self.printdvm('>>> Active job found')
            self.printdvm('file: %s' % self._fileActiveJob)
            self.printdvm(self._aj2text(jobdesc))
            # Return if the job is not in terminal state, or else do cleanup and
            # proceed with job submission.
            if jobsts.upper() not in self.jobstates['terminal']:
                # Job is not in terminal state for more than 1h or
                # its status was stopped to be updated for 1h.
                # Discard the job.
                # Submit the job cancel request and remove active job file.
                if lastststime - subtime > self._timeouts['JOB_GLOBAL']:
                    self.printdvm('>>> Job in non-terminal state for '+\
                                  '%i sec (allowed %i sec)' % (lastststime-subtime,
                                                               self._timeouts['JOB_GLOBAL']))
                    self._cancel_job(jobdesc['jobID'])
                    self._purge_job(jobdesc['jobID'])
                    self.printdvm(">>> Deleting the job's monitoring info from the UI.")
                    os.unlink(self._fileActiveJob)
                elif int(time.time()) - lastststime >= self._timeouts['JOB_GLOBAL']:
                    self.printdvm(">>> Job's status was not updated for more than %i sec " %\
                                  (self._timeouts['JOB_GLOBAL']))
                    self._cancel_job(jobdesc['jobID'])
                    self._purge_job(jobdesc['jobID'])
                    self.printdvm(">>> Deleting the job's monitoring info from the UI.")
                    os.unlink(self._fileActiveJob)
                else:
                    status = self.prev_status or 'OK'
                    s = 'Active job - %s [%s] %s' % \
                        (jobsts, time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime(lastststime)),
                                      jobdesc['jobID'])
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

        # resource endpoint discovery
        if not self.resource:
            self.printdvm('>>> Discover CREAM CE resource endpoint in BDII')
            ldap_filter = '(&(objectClass=GlueCE)(GlueCEImplementationName=CREAM)'+\
                '(GlueCEInfoHostName=%s)' % self.hostName +\
                '(|(GlueCEAccessControlBaseRule=VOMS:/%s/*)' % self.voName +\
                '(GlueCEAccessControlBaseRule=VO:%s)))' % self.voName
            ldap_attrlist = ['GlueCEUniqueID']
            try:
                tl = self._timeouts['ldap_timelimit']
            except KeyError:
                tl = None
            rc, qres = gridutils.query_bdii(ldap_filter, ldap_attrlist,
                                            ldap_timelimit=tl)
            if not rc:
                if qres[0] == 0: # empty set
                    sts = 'WARNING'
                else: # all other problems
                    sts = 'UNKNOWN'
                self.printd('%s'%(qres[2]))
                self.prints('%s'%(qres[1]))
                return sts
            resources = []
            for e in qres:
                resources.append(e[1]['GlueCEUniqueID'][0])
            self.printd('Discovered endpoints: \n%s' % '\n'.join(resources))
            self.resource = resources[0]
            self.printd('Endpoint to be used: %s' % self.resource)

        self.printdvm('>>> Preparing for job submission')

        # job's output directory
        if not os.path.isdir(self._dirJobOutput):
            try:
                os.makedirs(self._dirJobOutput)
            except OSError, e:
                status = 'UNKNOWN'
                summary = detmsg = status+": OSError: %s" % e
                return (status, summary, detmsg)

# TODO:
#        # build archive that will go to WN
#        rc, s, d = self._package_wnjob(probeshome)
#        if samutils.to_status(rc) != 'OK':
#            self.printd(d)
#            self.prints(s)
#            return rc
#
#        # prepare the script which will launch Nagios on WN
#        fpgridexe = probeshome+'/wnjob/'+self._fileWNExeName
#        if not os.path.exists(fpgridexe):
#            status = 'UNKNOWN'
#            msg = "%s: Path [%s] doesn't exist, while creating JDL." % \
#                        (status, fpgridexe)
#            return (status, msg, msg)
#        shutil.copy(fpgridexe, self._fileWNExeFPath)
#
#        # MB URI may come in the following format: proto://(...,...).
#        # We need to escape "()" for the shell on WN.
#        if self.mb_uri:
#            _mb_uri = self.mb_uri.replace('(','\(')
#            _mb_uri = "-b '"+_mb_uri.replace(')','\)')+"'"
#        else:
#            _mb_uri = ''

        # build JDL
#        if self.fqan:
#            _voms = "-f %s" % self.fqan
#        else:
#            _voms = ''
#        self._jdlPatterns['jdlArguments'] = \
#            "-v %(vo)s %(voms)s -d %(mb_destination)s %(mb_uri)s -t %(wnjobtimeout)s -w %(verb)s" % { \
#                 'vo' : self.voName,
#                 'voms' : _voms ,
#                 'mb_destination' : self.mb_destination,
#                 'mb_uri' : _mb_uri,
#                 'wnjobtimeout' : self._timeouts['WNJOB_GLOBAL']}
#        self._jdlPatterns['jdlExecutable'] = '/bin/hostname'
        self._build_jdl()

        # Check if jobs ID file is there. If, yes:
        # - cancel all the jobs defined there
        # - delete the file
        if os.path.exists(self._fileJobID):
            cmd = self.gridjobcmd['CANCEL']+' -i %s' % self._fileJobID
            # TODO: use this returned info later for more verbose output
            _rc, _summsg, _detmsg = self.run_cmd(cmd)
            os.unlink(self._fileJobID)

        # submit job
        verb = ''
        if self.verbosity >= probe.VERBOSITY_MAX:
            verb = '-d'
        cmd = '%s %s -r %s %s' % (self.gridjobcmd['SUBMIT'],
                                  verb,
                                  self.resource,
                                  self._fileJDL)
        self.printdvm('>>> Job submit command\n%s\n' % cmd)

        if self._nosubmit:
            #self._js_do_cleanup()
            self.printd('>>> Asked not to submit job. Bailing out')
            self.prints('Asked not to submit job. Bailing out')
            return 'OK'

        self.printdvm('>>> Submmitting grid job ... ', cr=False)
        rc, summary, detmsg = self.run_cmd(cmd, _verbosity=2)

        if rc != samutils.to_retcode('OK'):
            self.printdvm('failed.')
            self._js_do_cleanup()
            self.printd(detmsg)
            self.prints(summary)
            return rc
        self.printdvm('done.')

        self.printd(detmsg)

        # obtain job's ID
        try:
            jobid = re.search('^http.*CREAM.*$', detmsg, re.M).group(0).rstrip()
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
        jd['jobState']       = 'REGISTERED'
        jd['lastStateTimeStamp'] = stime
        self._save_activejob(jd)

        self.prints('Job was submitted [%s].' % jobid)

        return rc

    def metricDirectJobCancel(self):
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

    def _submit_results(self, host, mname, rc, summary, detmsg):
        """Submit check results.
        """
        metric_res = [{'host'    : host,
                       'service' : mname,
                       'status'  : str(rc),
                       'summary' : summary,
                       'details' : detmsg}]
        self._submit_service_checks(metric_res)

    def _thr_ajworker(self, jd):
        """Check status of submitted CREAM CE job and udate the grid
        *job status* (DirectJobSubmit) and/or *CE state* (DirectJobState).
        (The "update" is perfomed in the sense of Nagios passive checks.)

        - jd - job description
        """
        summary = ''
        detmsg = ''

        to = metricoutput.MetricOutputHandler()
        if self.passcheckdest == 'active':
            to.set_stream()
        def printd(dd, v=probe.VERBOSITY_MIN, cr=True, prep=False):
            """Prints string either to stdout or appends to a buffer.
            dd  - string
            v   - verbosity
            cr  - carriage return
            """
            if self.verbosity >= v:
                to.handle_detdata(dd, cr=cr, prep=prep)
        def printdvm(dd, cr=True, prep=False):
            """Invokes printd() with highest verbosity.
            """
            printd(dd, v=probe.VERBOSITY_MAX, cr=cr, prep=prep)
        def prints(s):
            to.handle_summary(s)
        def get_detdata():
            return to.get_detdata()
        def get_summary():
            return to.get_summary()

        def update_non_terminal(state, jd, ajf, detmsg=''):
            """Update active job file and job status passive check.
            """
            jd['jobState'] = state
            jd['lastStateTimeStamp'] = str(int(time.time()))
            s = self._save_activejob(jd, ajf)
            printdvm('>>> %s\nUpdated active job file\n%s\n' % (host, s))
            status = 'OK'
            summary = '%s: [%s] %s' % (status, state, jd['jobID'])
            self._submit_results(host, self.metJobState,
                                 str(samutils.to_retcode(status)),
                                 summary,
                                 '%s%s' % (summary, '\n'+detmsg or ''))

        def get_job_logging_info(jobid):
            if self.verbosity in [0, 1, 2]:
                vlevel = 1
            else:
                vlevel = 2
            cmd = '%s %i %s' % (self.gridjobcmd['LOGGING'], vlevel, jobid)
            rc, _, msg = self.thr_run_cmd(cmd, _verbosity=2)
            if samutils.to_status(rc) != 'OK':
                printd(">>> Failed to get job's logging info.")
            return (rc, '%s\n%s' % (cmd, msg))

        def job_discard_timedout(timeout, state, status='WARNING'):
            """Discard job because of job's timeout.
            """
            # cancel the job
            printd("%i min timeout in '%s' exceeded. Cancelling the job." % \
                        (timeout/60, state))
            _, msg = get_job_logging_info(jobid)
            printd(">>> Job's logging info: \n%s" % msg)
            printd('>>> Discard the job: Cancel/Purge from CE, delete from UI.')
            self._discard_job(state, jobid, ajf, timeout=False)
            smr = '[%s->Cancelled/Purged [timeout/dropped]]' % state
            prints('%s: %s' % (status, smr))
            printd(get_summary(), prep=True)
            printdvm('>>> Submit passive checks results: %s, %s' % (self.metJobState,
                                                                    srvdesc))
            # State [A/P]
            self._submit_results(host, self.metJobState, str(self.retCodes[status]),
                           get_summary(), get_detdata())
            # Submit [P]
            self._submit_results(host, srvdesc, str(self.retCodes[status]),
                           get_summary(), get_detdata())
            gridjobs_states[host] = smr
            sched.de_queue(host)
            return status

        subtime = int(jd['submitTimeStamp'])
        host    = jd['hostNameCE']
        srvdesc = jd['serviceDesc']
        jobid   = jd['jobID']

        workdir = '%s/%s' % (self.workdir_service, host)
        ajf = '%s/%s' % (workdir, self._fileActiveJobName)

        printdvm('>>> %s: Active job attributes' % host)
        printdvm('file: %s' % ajf)
        printdvm(self._aj2text(jd))

        global gridjobs_states

        cmd = self.gridjobcmd['STATUS']+' '+jobid
        printdvm('>>> %s: Status command\n%s' % (host, cmd))
        rc, summary, detmsg = self.thr_run_cmd(cmd, _verbosity=2)
        printdvm(detmsg)

        if rc != samutils.to_retcode('OK'):
            gridjobs_states[host] = 'Failure [get job status] : %s' % (detmsg)
            sched.de_queue(host)
            return (rc, summary, detmsg)

        # parse details output and determine the job's status
        match = re.search(r'^\s*Status\s*=\s*\[.*\]$', detmsg, re.M)

        if not match:
            status = 'UNKNOWN'
            # job was already purged from CE or ID is bad
            if re.search(r'job not found', detmsg, re.M):
                prints(status+': no such job on CE.')
                gridjobs_states[host] = 'Failure [no such job on CE]'
                self._unlink(ajf)
            else:
                prints(status+': unable to determine job status.')
                gridjobs_states[host] = 'Failure [unidentified job status]'
            printd(get_summary()+'\nJob id: '+jobid)
            # State [A/P]
            self._submit_results(host, self.metJobState, str(rc),
                                 get_summary(), get_detdata())
            sched.de_queue(host)
            return status

        currstat = match.group(0).rstrip()

        printdvm('>>> %s: %s %s' % (host,
                            time.strftime("%Y-%m-%dT%H:%M:%SZ"),currstat))

        # TODO: check times "lastStateTimeStamp - submitTimeStamp" for jobs not in terminal states

        #non_terminal_re = '('+'|'.join([ s.title() for s in jobstates['non_terminal'] ])+')'
        #timeout_JOB_GLOBAL = self._timeouts['JOB_GLOBAL']
        currtime = int(time.time())

        if re.search('DONE-(OK|FAILED)', currstat):
            '''OK for all Reasons.
            '''
            jobstate = 'DONE'
            status = 'OK'
            djo = '%s/%s' % (workdir, self._dirJobOutputName)
            # create the directory where output file is stored
            dirname = jobid[8:].replace(":", "_").replace("/", "_")
            fjo = '%s/%s/%s' % (djo, dirname, self._fileJobOutputName)
            printdvm('>>> %s:\n' % host+\
                          'Job is in %s state.\n' % jobstate+\
                          'Purge the job and submit passive check result to '+\
                          '\n<%s,%s>\n<%s,%s>\n' % (host, srvdesc,
                                                    host, self.metJobState))
            printdvm('Retrieving job output...')
            self._getoutput(jobid, djo)
            printdvm('Purging the job... ')
            self._purge_job(jobid, timeout=False)

            printdvm('\nJob output:')
            try:
                for ln in open(fjo,'r').readlines():
                    printdvm(ln)
            except IOError:
                printdvm('Could not open job output file: '+fjo)

            prints('%s: %s.' % (status, jobstate))

            summary = get_summary()
            detdata = '%s\n%s' % (get_summary(), get_detdata())
            # Submit [P]
            self._submit_results(host, srvdesc, str(rc), summary, detdata)
            # State [A/P]
            self._submit_results(host, self.metJobState, str(rc), summary,
                                 detdata)
            self._unlink(ajf)
            gridjobs_states[host] = jobstate
            sched.de_queue(host)
            return status

        elif re.search('ABORTED', currstat):
            jobstate = 'ABORTED'
            status = 'CRITICAL'
            prints('%s: %s' % (status, jobstate))
            printd(get_summary(), prep=True)

            _, msg = get_job_logging_info(jobid)
            printd(msg)

            # Submit [P]
            self._submit_results(host, srvdesc, str(self.retCodes[status]),
                           get_summary(), get_detdata())
            # State [A/P]
            self._submit_results(host, self.metJobState, str(self.retCodes[status]),
                           get_summary(), get_detdata())
            printdvm('>>> Discard job [Cancel/Purge from CE; delete from UI]')
            self._discard_job(jobstate, jobid, ajf, timeout=False)
            gridjobs_states[host] = 'Aborted'
            sched.de_queue(host)
            return status

        elif re.search('CANCELLED', currstat):
            # Job was cancelled, but active job file was left. Seems like it wasn't
            # us who did delete the file.
            printdvm('>>> %s: Warning. Job was cancelled, ' % host +\
                          'but active job file was left\n%s' % ajf)
            jobstate = 'CANCELLED'
            self._discard_job(jobstate, jobid, ajf, timeout=False)

        elif re.search('REGISTERED', currstat):
            # TODO: process_on_state('REGISTERED', status='CRITICAL')
            jobstate = 'REGISTERED'
            if currtime - subtime >= self._timeouts['JOB_'+jobstate]:
                return job_discard_timedout(self._timeouts['JOB_'+jobstate],
                                          jobstate, status='CRITICAL')
            update_non_terminal(jobstate, jd, ajf, detmsg=detmsg)
            jobstate = '[%s]'%jobstate

        elif re.search('PENDING', currstat):
            # TODO: process_on_state('PENDING', status='CRITICAL')
            jobstate = 'PENDING'
            if currtime - subtime >= self._timeouts['JOB_'+jobstate]:
                return job_discard_timedout(self._timeouts['JOB_'+jobstate],
                                           jobstate, status='WARNING')
            update_non_terminal(jobstate, jd, ajf, detmsg=detmsg)
            jobstate = '[%s]'%jobstate

        elif re.search('IDLE', currstat):
            # TODO: process_on_state('IDLE', status='CRITICAL')
            jobstate = 'IDLE'
            if currtime - subtime >= self._timeouts['JOB_'+jobstate]:
                return job_discard_timedout(self._timeouts['JOB_'+jobstate],
                                           jobstate, status='WARNING')
            update_non_terminal(jobstate, jd, ajf, detmsg=detmsg)
            jobstate = '[%s]'%jobstate

        elif re.search('\[RUNNING', currstat):
            # TODO: process_on_state('RUNNING', status='CRITICAL')
            jobstate = 'RUNNING'
            if currtime - subtime >= self._timeouts['JOB_'+jobstate]:
                return job_discard_timedout(self._timeouts['JOB_'+jobstate],
                                           jobstate, status='CRITICAL')
            update_non_terminal(jobstate, jd, ajf, detmsg=detmsg)
            jobstate = '[%s]'%jobstate

        elif re.search('REALLY-RUNNING', currstat):
            jobstate = 'REALLY-RUNNING'
            if currtime - subtime >= self._timeouts['JOB_'+jobstate]:
                return job_discard_timedout(self._timeouts['JOB_'+jobstate],
                                           jobstate, status='CRITICAL')
            update_non_terminal(jobstate, jd, ajf, detmsg=detmsg)
            jobstate = '[%s]'%jobstate

        elif re.search('HELD', currstat):
            jobstate = 'HELD'
            if currtime - subtime >= self._timeouts['JOB_'+jobstate]:
                return job_discard_timedout(self._timeouts['JOB_'+jobstate],
                                           jobstate, status='CRITICAL')
            update_non_terminal(jobstate, jd, ajf, detmsg=detmsg)
            jobstate = '[%s]'%jobstate

        elif re.search('UNKNOWN', currstat):
            jobstate = 'UNKNOWN'
            printd('>>> %s: Warning. Job is in %s: %s' % (host, jobstate,
                                                            jobid))
            printdvm('>>> Discard the job.')
            self._discard_job(jobstate, jobid, ajf, timeout=False)
            update_non_terminal(jobstate, jd, ajf, detmsg=detmsg)
            jobstate = '[%s]'%jobstate

        else:
            jobstate = 'UNDETERMINED'
            printd('>>> %s: Warning. Job is in %s: %s' % (host, jobstate,
                                                            jobid))
            if currtime - subtime >= self._timeouts['JOB_GLOBAL']:
                return job_discard_timedout(self._timeouts['JOB_GLOBAL'],
                                          jobstate, status='UNKNOWN')
            update_non_terminal(jobstate, jd, ajf, detmsg=detmsg)
            jobstate = '[%s]'%jobstate

        gridjobs_states[host] = jobstate
        sched.de_queue(host)
        return rc

    def metricDirectJobMonit(self):
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



