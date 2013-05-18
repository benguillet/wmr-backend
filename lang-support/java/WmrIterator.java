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

import java.util.*;

public class WmrIterator implements Iterator<String>, Iterable<String> {
	
	String myKey;

	WmrIterator(String key) {
		myKey = key;
	}

	public WmrIterator iterator() {
		return this;
	}
	
	public boolean hasNext() {
		return !WmrReducerImpl.endInput() && (WmrReducerImpl.nextKey.equals(myKey));
	}

	/** Delivers next (i.e., first undelivered) value from input key-value pairs*/
	public String next() throws NoSuchElementException {
		if (!hasNext()) 
			throw new NoSuchElementException("No more key-value pairs for this key");
		String value = WmrReducerImpl.nextValue;
		WmrReducerImpl.advance();
		return value; 
	}

	public void remove() throws UnsupportedOperationException {
		throw new UnsupportedOperationException(
			"Method remove() is not implemented in the class WmrIterator");
	}
}



