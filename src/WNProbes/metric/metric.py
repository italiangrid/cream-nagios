
"""MetricOutput class.
Produces WLCG compliant messages.
"""

import socket
import time

class MetricOutput(object):
    ''
    keys_req = ['service_uri',
            'service_flavour',
            'host_name',
            'metric',
            'status',
            'summary',
            'site']
    def __init__(self, body):
        for k in self.keys_req:
            if not body.has_key(k):
                raise KeyError("'%s' should be provided." % k)
        self.body_dict = body
        if not self.body_dict.has_key('details'):
            self.body_dict['details'] = ''
        self.body_dict['timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                    time.gmtime(time.time()))
        self.body_dict['gatheredAt'] = socket.gethostname()

    def wlcg_format(self):
        message = "serviceURI: %s\n" % self.body_dict['service_uri'] +\
        "hostName: %s\n" % self.body_dict['host_name'] +\
        "serviceFlavour: %s\n" % self.body_dict['service_flavour'] +\
        "siteName: %s\n" % self.body_dict['site'] +\
        "metricStatus: %s\n" % self.body_dict['status'] +\
        "metricName: %s\n" % self.body_dict['metric'] +\
        "summaryData: %s\n" % self.body_dict['summary'] +\
        "gatheredAt: %s\n" % self.body_dict['gatheredAt'] +\
        "timestamp: %s\n" % self.body_dict['timestamp']
        try:
            message += "nagiosName: %s\n" % self.body_dict['nagios_name']
        except KeyError: pass
        try:
            message += "role: %s\n" % self.body_dict['role']
        except KeyError: pass
        try:
            message += "voName: %s\n" % self.body_dict['vo']
        except KeyError: pass
        try:
            message += "serviceType: %s\n" % self.body_dict['service_type']
        except KeyError: pass
        if self.body_dict.has_key('encrypted') and self.body_dict['encrypted']:
            message += "encrypted: %s\n" % self.body_dict['encrypted']
        if self.body_dict.has_key('details') and self.body_dict['details']:
            message += "detailsData: %s\n" % self.body_dict['details']
        message += "EOT\n"
        return message
