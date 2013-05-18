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

/**
 * wmr_common.h:
 *  Constants and functions shared by the mapper and reducer implementations,
 *  but not accessible by the user.
 */

#ifndef _wmr_common_h
#define _wmr_common_h

#define BUFFSIZE 1024 * 1024
#define DELIMITER '\t'

/**
 * Split the line into a key and value based on a globally-defined
 * delimiter. Return 0 if the line was empty, nonzero otherwise.
 * 
 * The contents of line will be modified, and the key and val pointers will
 * point to their appropriate starting positions within line. If no
 * delimiter is found, then val will point to the empty string and key will
 * contain the entire line. Neither key nor val will contain the a terminal
 * newline, if present in the original line.
 *
 * Unlike strtok, this function is completely reentrant.
 */
int split_kv_pair(char* line, char** key, char** val);

#endif
