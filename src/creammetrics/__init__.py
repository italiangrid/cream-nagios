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
Set of Nagios probes for testing grid services.

Contains the following Nagios probes:
CREAMCE-probe, CREAMCEDJS-probe and the script 
samtest-run.

- The probes can run in active and/or passives modes (in Nagios sense).
  Publication of passive test results from inside of probes can be done
  via Nagios command file or NSCA.
- On worker nodes Nagios is used as probes scheduler and executer. Metrics
  results from WNs are sent to Message Broker.

.. packagetree::
   :style: UML
"""

__docformat__ = 'restructuredtext en'

__all__ = ["CreamDjsMetrics", "CreamMetrics"]


