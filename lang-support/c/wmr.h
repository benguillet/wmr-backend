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
 * wmr.h:
 *  Public interface for the WMR C API.
 */

#ifndef _wmr_h
#define _wmr_h


/**
 * This "handle" type is used to retrieve values in the reducer. It is
 * passed to the reducer function as an argument, and passed to
 * wmr_get_val(wmr_handle) to get the actual value.
 */
typedef const int wmr_handle;

/**
 * Retrieve the next value associated with the current key. Returns NULL if
 * no more values are available.
 *
 * NOTE: The pointer that is returned is only valid until the next call to
 *  this function. This allows the storage space to be reused, and avoids
 *  having to explicitly manage memory in the reducer function.
 */
const char* wmr_get_val(wmr_handle h);

/** Emit a key-value pair from the mapper or reducer. */
void wmr_emit(const char* key, const char* val);


/**
 * This is the signature for the mapper function that the user should 
 * implement.
 */
void mapper(char* key, char* val);

/**
 * This is the signature for the reducer function that the user should
 * implement.
 */
void reducer(char* key, wmr_handle h);

#endif
