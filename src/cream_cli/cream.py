#!/usr/bin/env python
"""
Copyright (c) Members of the EGEE Collaboration. 2006-2010.
See http://www.eu-egee.org/partners/ for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

--------------------
The CREAM CE client. 
--------------------
"""
__author__ = "Lisa Zangrando lisa.zangrando@pd.infn.it"
__date__ = "27.09.2013"
__version__ = "0.1.0"

from optparse import OptionParser, OptionGroup
from urlparse import urlparse
import signal, subprocess, shlex, sys, time, string, os


class Client(object):
    # Default return values for Nagios
    OK       = 0
    WARNING  = 1
    CRITICAL = 2
    UNKNOWN  = 3
    
    # Default values for the different options and Constants    
    DEFAULT_PORT = 8443
    DEFAULT_TIMEOUT = 120
    DEFAULT_VERBOSITY = False 

    # Variables
    usage = "usage %prog [options]"
    # probeName = "CREAMProbe"
    optionParser = None
    options = None
    dir = None
    args = None
    url = None
    hostname = None
    port = None
    jdl = None
    queue = None
    lrms = None
    timeout = DEFAULT_TIMEOUT
    verbose = DEFAULT_VERBOSITY
    fullOptional = None


    def __init__(self, name, version):
        #signal.signal(signal.SIGALRM, self.sig_handler)
        #signal.signal(signal.SIGTERM, self.sig_handler)
        self.name = name
        self.version = version
        self.optionParser = OptionParser(version="%s v.%s" % (self.name, "1.0"))


    # return Values for Nagios
    def nagiosExit(self, exitCode, msg):
        print msg
        exit(exitCode)


    # read out the options from the command-line
    def createParser(self, fullOptional = "TRUE"):
        self.fullOptional = fullOptional

        optionParser = self.optionParser
        optionParser.add_option("-H",
                      "--hostname",
                      dest="hostname",
                      help="The hostname of the CREAM service.")
    
        optionParser.add_option("-p",
                      "--port",
                      dest="port",
                      help="The port of the service. [default: %default]")
                      
        optionParser.add_option("-x",
                      "--proxy",
                      dest="proxy",
                      help="The proxy path")
        
        optionParser.add_option("-t",
                      "--timeout",
                      dest="timeout",
                      help="Probe execution time limit. [default: %default sec]",
                      default = self.DEFAULT_TIMEOUT)
        
        optionParser.add_option("-v",
                      "--verbose",
                      action="store_true",
                      dest="verbose",
                      help="verbose mode [default: %default]",
                      default = self.DEFAULT_VERBOSITY)
 
        if fullOptional == "TRUE":
            optionParser.add_option("-u",
                      "--url",
                      dest="url",
                      help="The status endpoint URL of the service. Example: https://<host>[:<port>]/cream-<lrms>-<queue>")

            optionParser.add_option("-l",
                          "--lrms",
                          dest="lrms",
                          help="The LRMS name (e.g.: 'lsf', 'pbs' etc)")

            optionParser.add_option("-q",
                          "--queue",
                          dest="queue",
                          help="The queue name (e.g.: 'creamtest')")
    
            optionParser.add_option("-j",
                          "--jdl",
                          dest="jdl",
                          help="The jdl path")

            if self.name == "cream-jobOutput":
                optionParser.add_option("-d",
                              "--dir",
                              dest="dir",
                              default = "/var/lib/argo-monitoring/eu.egi.CREAMCE",
                              help="The output sandbox path")

        else:
            optionParser.add_option("-u",
                      "--url",
                      dest="url",
                      help="The status endpoint URL of the service. Example: https://<host>[:<port>]")



    def readOptions(self):
        optionParser = self.optionParser
        
        (self.options, self.args) = optionParser.parse_args()
       
        if not self.options.url and not self.options.hostname:
            optionParser.error("Specify either option -u URL or option -H HOSTNAME (and -p PORT) or read the help (-h)")

        if self.options.url and self.options.hostname:
            optionParser.error("Options -u URL and -H HOSTNAME are mutually exclusive")

        if self.options.url:
            o = urlparse(self.options.url)

            if o.scheme != "https":
                optionParser.error("wrong URL scheme (use 'https://<hostname>[:<port>]/cream-<lrms-system-name>-<queue-name>')")
            
            if not o.hostname:
                optionParser.error("hostname not specified (use 'https://<hostname>[:<port>]/cream-<lrms-system-name>-<queue-name>')")
            else:
                self.hostname = o.hostname

            if not o.port:
                optionParser.error("port not specified (use 'https://<hostname>[:<port>]/cream-<lrms-system-name>-<queue-name>')")
            else:
                self.port = o.port

            if o.path:
                if self.fullOptional == "FALSE":
                    optionParser.error("wrong URL (use 'https://<hostname>[:<port>]')")
                else:
                    s = o.path.split("-")

                    if len(s) != 3 or s[0] != "/cream":
                        optionParser.error("wrong path (use 'https://<hostname>[:<port>]/cream-<lrms-system-name>-<queue-name>')")

                    self.lrms = s[1]
                    self.queue = s[2]

        # CE_ID FORMAT: https://<host>[:<port>]/cream-<lrms-system-name>-<queue-name>
        if self.options.hostname:
            if ":" in self.options.hostname:
                optionParser.error("malformed hostname: port definition not allowed (use -p option)")
            else:
                self.hostname = self.options.hostname

        if self.options.port:
            if self.port:
                optionParser.error("port already defined in the URL")
            else:
                self.port = self.options.port
        elif not self.port:
                self.port = DEFAULT_PORT

        if self.options.verbose:
            self.verbose = self.options.verbose
 
        if self.options.proxy:
            os.environ["X509_USER_PROXY"] = self.options.proxy


        if self.options.timeout:
            self.timeout = self.options.timeout

        signal.signal(signal.SIGALRM, self.sig_handler)
        signal.alarm(int(self.timeout)) # triger alarm in n seconds

        if self.fullOptional == "TRUE":
            if self.options.lrms:
                if self.lrms:
                    optionParser.error("lrms name already defined in the URL")
                else:
                    self.lrms = self.options.lrms
            elif not self.lrms:
                    optionParser.error("lrms name not specified!")

            if self.options.queue:
                if self.queue:
                    optionParser.error("queue name already defined in the URL")
                else:
                    self.queue = self.options.queue
            elif not self.queue:
                    optionParser.error("queue name not specified!")

            if not self.options.jdl:
                optionParser.error("JDL not specified!")
            else:
                self.jdl = self.options.jdl

            self.url = self.hostname + ":" + str(self.port) + "/cream-" + self.lrms + "-" + self.queue
            #self.url = "%(hostname)s:%(port)s/cream-%(lrms)-%(queue)" % {'hostname': self.url, 'port': self.port, 'lrms': self.lrms, 'queue': self.queue}
  
            if self.name == "cream-jobOutput" and self.options.dir:
                    self.dir = self.options.dir 
        else:
            self.url = self.hostname + ":" + str(self.port)



    # set-up the signal-handlers                        
    def sig_handler(self, signum, frame):
        if signum == signal.SIGALRM:
            self.nagiosExit(self.WARNING, "Timeout occurred (" + str(self.timeout) + " sec)")
        elif signum == signal.SIGTERM:
            self.nagiosExit(self.WARNING, "SIGTERM received!")



    # Prints debug information into stderr if verbose is enabled
    def debug(self, msg):
       #Prints debug information into stderr if verbose is enabled
       if self.verbose:
           print >> sys.stderr, msg



    #Check whether the proxy exists and if it has any time left. 
    def checkProxy(self):
        if not os.environ.has_key("X509_USER_PROXY"):
            raise Exception("X509_USER_PROXY not set")
    
        if not os.path.exists(os.environ["X509_USER_PROXY"]):
            raise Exception("Proxy file not found or not readable")

        cmd="/usr/bin/voms-proxy-info -timeleft"

        timeLeft = self.execute(cmd)
        
        if timeLeft <= 0 :
            raise Exception("No proxy time left")



    #Submit a job to CREAM with automatic delegation and return its job id.
    def jobSubmit(self):
        cmd="/usr/bin/glite-ce-job-submit -d -a -r " + self.url + " " + self.jdl
        output = self.execute(cmd)

        self.debug(output)

        for elem in output:
            if string.find(elem, "ERROR") > 0:
                raise Exception(elem)


        """
        self.debug(cmd)

        args = shlex.split(cmd.encode('ascii'))

        proc = subprocess.Popen(args , shell=False , stderr=subprocess.STDOUT , stdout=subprocess.PIPE)
        fPtr=proc.stdout
        retVal=proc.wait()
        output=fPtr.readlines()

        self.debug(output)

        #if retVal != 0:
        if "error" in ','.join(output) or "fault" in ','.join(output) or retVal != 0:
            self.nagiosExit(self.CRITICAL, "Job submission failed with return value: " + str(proc.returncode) + " \nCommand reported: " +  ','.join(output))
        """

        jobId=output[-1] #if job submission was succesfull (at this point of code,it is),then the last line of output holds the job id
        jobId=jobId[:-1] #to remove the trailing '\n'
        return jobId


    #Retrieve the job status.
    def jobStatus(self, jobId):
        self.debug("invoking jobStatus")

        cmd = "/usr/bin/glite-ce-job-status " + jobId

        output = self.execute(cmd)

        self.debug(output)
        
        status = None
        for i in output:
            if "Status" in i:
                status=i

        if not status:
            raise Exception("Status couldn't be determined for jobId " + jobId + ". Command reported: " + ','.join(output))

        jobStatus = status.split('[')
        jobStatus = jobStatus[1].split(']')

        exitCode = None
        for y in output:
            if "ExitCode" in y:
                exitCode = y

        if exitCode:
            self.debug("exitCode="+exitCode)

            jobExitCode = exitCode.split('[')
            jobExitCode = jobExitCode[1].split(']')

            return jobStatus[0], jobExitCode[0]

        return jobStatus[0], -1



    #Cancel the job.
    def jobCancel(self, jobId):
        self.debug("invoking jobCancel")

        cmd="/usr/bin/glite-ce-job-cancel --noint " + jobId

        output = self.execute(cmd)

        for elem in output:
            if string.find(elem, "job not found") > 0:
                raise Exception("Job " + jobId + " not found!")



    #Purge the job.
    def jobPurge(self, jobId):
        self.debug("invoking jobPurge")

        cmd="/usr/bin/glite-ce-job-purge --noint " + jobId
        output = self.execute(cmd)

        for elem in output:
            if string.find(elem, "job not found") > 0:
                raise Exception("Job " + jobId + " not found!")



    #Service info.
    def serviceInfo(self):
        self.debug("invoking service info")

        cmd="/usr/bin/glite-ce-service-info " + self.url
        output = self.execute(cmd)
        info = ""

        for elem in output:
            info += elem

        return info



    #Allowed submission.
    def allowedSubmission(self):
        self.debug("invoking allowedSubmission")

        cmd="/usr/bin/glite-ce-allowed-submission " + self.url
        output = self.execute(cmd)

        for elem in output:
            if string.find(elem, "enabled") > 0:
                return "ENABLED"
            elif string.find(elem, "disabled") > 0:
                return "DISABLED"
       
        raise Exception(output)


    #Get Output Sandbox.
    def getOutputSandbox(self, jobId):
        self.debug("invoking getOutputSandbox")

        cmd="/usr/bin/glite-ce-job-output --noint"

        if self.dir:
            cmd += " --dir " + self.dir

        cmd += " " + jobId
        output = self.execute(cmd)

        for elem in output:
            if string.find(elem, "UBERFTP ERROR OUTPUT") > 0:
                raise Exception("cannot retrieve the output sandbox")
            elif string.find(elem, "output") > 0:
                result = elem[elem.find("dir "):elem.find("\n")]
                if result:
                    return result[4:]
                else:
                    return "n/a"


    #Execute command.
    def execute(self, command):
        self.debug("executing command: " + command)

        args = shlex.split(command.encode('ascii'))

        proc = subprocess.Popen(args , stderr=subprocess.STDOUT , stdout=subprocess.PIPE)
        fPtr = proc.stdout
        retVal = proc.wait()
        output = fPtr.readlines()

        if retVal != 0:
            raise Exception("command '" + command + "' failed: return_code=" + str(proc.returncode) + "\ndetails: " + repr(output))

        for elem in output:
            if "ERROR" in elem or "FATAL" in elem or "FaultString" in elem or "FaultCode" in elem or "FaultCause" in elem:
                raise Exception("command '" + command + "' failed: return_code=" + str(proc.returncode) + "\ndetails: " + repr(output))

        return output

"""
def main():
    probe = CREAMDirectJobSubmissionProbe()
    probe.createParser()
    probe.readOptions()

    try:
        probe.checkProxy()

        jobId = probe.jobSubmit()

        probe.debug(jobId)

        time.sleep(10)

        probe.jobCancel(jobId)

        time.sleep(20)

        probe.jobPurge(jobId)
    except Exception as ex:
        probe.nagiosExit(probe.CRITICAL, ex)

    terminalStates = ['DONE-OK', 'DONE-FAILED', 'ABORTED', 'CANCELLED']
    lastStatus=""

    while not lastStatus in terminalStates:
        time.sleep(10)
        try:
            lastStatus = probe.jobStatus(jobId)
        except Exception as ex:
            probe.nagiosExit(probe.CRITICAL, ex)
           
    probe.jobPurge(jobId)

    if lastStatus == terminalStates[0]:
        probe.nagiosExit(probe.OK, "Job terminated with status " + lastStatus)
    else:
        probe.nagiosExit(probe.CRITICAL, "Job terminated with status " + lastStatus)



if __name__ == '__main__':
    main()
"""
