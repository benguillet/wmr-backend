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

int split_kv_pair(char* line, char** key, char** val)
{
    char* sep;
    
    /* The key always starts at the beginning of the line */
    *key = line;
    
    /* Seek to a delimiter or terminal character */
    sep = line;
    while (*sep != DELIMITER && *sep != '\n' && *sep != '\0')
        sep++;
    if (*sep == DELIMITER)
    {
        /* Split into two strings by changing the delimiter into a null */
        *sep = '\0';
        *val = ++sep;
    }
    else
    {
        /* If no delimiter, use an empty value */
        *val = "";
    }
    
    /* Obliterate the newline at the end if present */
    while (*sep != '\n' && *sep != '\0')
        sep++;
    if (*sep == '\n')
        *sep = '\0';
    
    /* Check whether the line was empty and return */
    return (sep != line);
}
