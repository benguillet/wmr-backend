/*
 * Copyright 2010 WebMapReduce Developers
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

import java.io.*;
import java.util.*;

public class WmrReducerImpl extends WmrCommonImpl {

  static String nextKey = null; 
  static String nextValue = null;  /* waits to be delivered by WmrIterator.next() */
  // Invariant:  after constructor, nextKey == null  iff  nextValue == null 
  //     iff  end of input

  static {
    advance();
  }

  static boolean endInput() {
    return nextKey == null;
  }

  /** Fetch next nextKey and nextValue.  
      If end of input stream, assign null to nextKey and nextValue. */
  static void advance() {
    String line = null;
    try {
      line = getLine();
    } catch (IOException ex) {
      throw new RuntimeException(
        "IOException when trying to read a key-value pair", ex);
    }
    if (line == null) {
      // no more input
      nextKey = nextValue = null;
      return;
    }
    // assert -- a fresh line was successfully fetched
    int delimIndex = line.indexOf(delim);
    if (delimIndex == -1) {
      nextKey = line;
      nextValue = "";
    } else {
			nextKey = line.substring(0,delimIndex);
			nextValue = line.substring(delimIndex+1);
    }
  }

  public static void main(String[] args) 
    throws IOException, NoSuchElementException
	{
    Reducer red = new Reducer();
    while (!endInput())
      red.reduce(nextKey, new WmrIterator(nextKey));
  }
}
