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

var stdin = process.stdin;
var stdout = process.stdout;
var data   = '';
stdin.setEncoding('utf8');
stdin.resume();

stdin.on('data', function(chunk) {
    if (chunk) {
        data += chunk;
        data = data.replace(/(\n|\r|\r\n)$/, '\n');

        while (data.indexOf('\n') > -1) {
            var i = data.indexOf('\n') + 1;
            var line = data.slice(0, i);
            var partition = Wmr.parseInput(data.slice(0, i));
            //console.log(partition);
            if (partition.length === 2) {
                mapper(partition[0], partition[1]);
            } else if (partition.length === 1) {
                mapper(partition[0]);
            }
            data = data.slice(i);
        }
    }
});

stdin.on('end', function() {
   if (data) {
        var partition = Wmr.parseInput(data);
        if (partition.length === 2) {
            mapper(partition[0], partition[1]);
        } else if (partition.length === 1) {
            mapper(partition[0]);
        }
    }
});
