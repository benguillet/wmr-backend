// Copyright 2013 Benjamin Guillet, WebMapReduce Developers
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


var default_separator = '\t';

exports.emit = function(key, value) {
    console.log(key + default_separator + value);
};

exports.parseInput = function(line, separator) {
    separator = typeof separator !== 'undefined' ? separator : default_separator;
    if (line && line.trim().length > 0) {
        var s = line.trim().split(separator);
        return s;
    }
    return [];
};
