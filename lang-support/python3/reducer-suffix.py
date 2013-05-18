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

from itertools import groupby
from operator import itemgetter
import sys
from wmr import common

# input comes from STDIN (standard input)
data = map(common.parse_input, sys.stdin)
# groupby groups multiple word-count pairs by word,
# and creates an iterator that returns consecutive keys and their group:
#   current_word - string containing a word (the key)
#   group - iterator yielding all ["<current_word>", "<count>"] items
for current_word, group in groupby(data, itemgetter(0)):
    reducer(current_word, map(itemgetter(1), group))
