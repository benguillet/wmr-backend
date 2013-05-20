// Copyright 2013 Benjamin Guillet, WebMapReduce Developers
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var stdin = process.stdin;
var stdout = process.stdout;
var data   = '';
var info = {};
stdin.setEncoding('utf8');
stdin.resume();

stdin.on('data', function(chunk) {
    if (chunk) {
        data += chunk;
        data = data.replace(/\r\n/g, '\n');

        while (data.indexOf('\n') > -1) {
            var i = data.indexOf('\n') + 1;
            var partition = Wmr.parseInput(data.slice(0, i));
            var key = partition[0];
            var value = partition[1];
            if (!info[key]) {
                info[key] = [];
                info[key].push(value);
            } else {
                info[key].push(value);
            }
            data = data.slice(i);
        }
    }
});

stdin.on('end', function() {
    Object.keys(info).forEach(function(key) {
        reducer(key, info[key]);
    });
});
