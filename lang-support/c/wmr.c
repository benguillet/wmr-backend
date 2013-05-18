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

#include "wmr_common.h"
#include <stdio.h>

/* NOTE: wmr_get_val(const char*) is defined in wmr_reducer.c */

void wmr_emit(const char* key, const char* val)
{
    printf("%s%c%s\n", key, DELIMITER, val);
}
