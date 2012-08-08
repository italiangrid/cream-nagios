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
JDL related classes.

JDL class for templated substitution.

Konstantin Skaburskas <konstantin.skaburskas@cern.ch>, CERN
SAM (Service Availability Monitoring)
"""

from gridmon.template import TemplatedFile
from gridmon.metricoutput import OutputHandlerSingleton

class JDLTemplate(OutputHandlerSingleton):
    '''Provides templated JDL substitutions.
    '''
    def __init__(self, file_templ, file, mappings={}):
        OutputHandlerSingleton.__init__(self)
        
        self.__tmpl = TemplatedFile(file_templ, file, mappings)      

    def build_save(self):
        """Substitute and save. 
        """
        tmpl = self.__tmpl
        self.printdvm('# Template file: %s' % tmpl.file_templ)
        
        tmpl.load()
        self.printdvm('# Template:\n%s' % tmpl.template)
        
        self.printdvm('# Mappings for template substitutions:\n%s' % 
            '\n'.join(['%s : %s'%(k,v) for k,v in tmpl.mappings.items()]))
        tmpl.subst()
        self.printdvm('# Resulting substitutions:\n%s' % tmpl.substitution)
        
        self.printdvm('# Saving to %s ... ' % tmpl.file, cr=False)
        tmpl.save()
        self.printdvm('done.')


