/* Copyright 2012 WebMapReduce developers

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */

#include "wmr.h"
#include "wmr_common.h"
#include <stdio.h>

void run_mapper(void);


int main()
{
    run_mapper();
    return 0;
}

void run_mapper(void)
{
    char line[BUFFSIZE];
    char* key;
    char* val;
    
    /* Run the mapper on all non-empty lines of the input */
    while (fgets(line, BUFFSIZE, stdin) != NULL)
    {
        if (split_kv_pair(line, &key, &val))
            mapper(key, val);
    }
}
