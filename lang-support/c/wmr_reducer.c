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
#include <string.h>
#include <stdio.h>

/* Used by wmr_get_val in wmr.c, so we can't declare this static. */
void reducer_get_next(void);

static void run_reducer(void);
static void swap(char**, char**);

/* We maintain two separate buffers so that we can both save the current full
 * line and maintain a pointer to the last value (to pass to the user). */
static char  reducer_buff1[BUFFSIZE];
static char  reducer_buff2[BUFFSIZE];
static char  reducer_last_key[BUFFSIZE];
static char* reducer_last_line = reducer_buff1;
static char* reducer_line      = reducer_buff2;
static char* reducer_key       = NULL;
static char* reducer_val       = NULL;
static int   reducer_key_index = 0;


int main()
{
    run_reducer();
    return 0;
}


void run_reducer(void)
{
    char key[BUFFSIZE];
    
    /* "Prime" the buffers by getting the first key/value pair in the input */
    reducer_get_next();
    
    /* reducer_key will point to the next key until EOF is reached */
    while (reducer_key != NULL)
    {
        strcpy(key, reducer_key);
        reducer(key, reducer_key_index);
    }
}

/**
 * Get the next value associated with the given key. Returns NULL if no more
 * values are available. This is part of the user-facing API (declared in
 * wmr.h).
 */
const char* wmr_get_val(wmr_handle h)
{
    /* Check that we haven't reached the end of the input and that the
     * passed key matches the current one */
    if (reducer_key == NULL || h != reducer_key_index)
        return NULL;
    
    char* val = reducer_val;
    reducer_get_next();
    return val;
}

/**
 * Read the next line of input into the global key/value buffers. If a line
 * was read successfully, then after this function is called:
 *  - reducer_line points to the line read by this function
 *  - reducer_key points to the key
 *  - reducer_val points to the value
 *  - reducer_last_key compares equal to reducer_key
 *  - reducer_key_index will be incremented if the key read was not equal
 *    to the last key before this call
 *
 * If the end of the input is reached before a new line is read, then both
 * reducer_key and reducer_val will be NULL.
 */
void reducer_get_next(void)
{
    swap(&reducer_last_line, &reducer_line);
    
    /* Read until EOF or a non-empty line */
    while (1)
    {
        if (fgets(reducer_line, BUFFSIZE, stdin) == NULL)
        {
            /* Signal EOF by setting pointers to null */
            reducer_key = NULL;
            reducer_val = NULL;
            return;
        }
        
        if (split_kv_pair(reducer_line, &reducer_key, &reducer_val))
        {
            /* Check whether key changed since last */
            if (strcmp(reducer_last_key, reducer_key) != 0)
            {
                strcpy(reducer_last_key, reducer_key);
                reducer_key_index++;
            }
            
            return;
        }
    }
}

void swap(char** one, char** two)
{
    char* tmp = *one;
    *one = *two;
    *two = tmp;
}
