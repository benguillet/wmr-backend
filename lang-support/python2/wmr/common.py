# Copyright 2010 WebMapReduce Developers
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

separator = '\t'

class Wmr:
    @classmethod
    def emit(cls, key, value):
        print '%s%s%s' % (key, separator, value)

def parse_input(line, separator='\t'):
    line.rstrip('\n')
    # does not use partition because it was added in Python 2.5
    return (line[:line.find(separator)], line[line.find(separator)+1:])
