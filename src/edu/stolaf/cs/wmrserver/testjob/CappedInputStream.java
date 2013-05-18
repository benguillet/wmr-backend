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

package edu.stolaf.cs.wmrserver.testjob;

import java.io.InputStream;
import java.io.IOException;
import org.apache.commons.io.input.CountingInputStream;

public class CappedInputStream extends CountingInputStream
{
	private long _maximumCount;

	public CappedInputStream(InputStream input, long maximumCount)
	{
		super(input);
		_maximumCount = maximumCount;
	}

	public int read() throws IOException
	{
		long count = getByteCount();
		if (count < _maximumCount)
			return super.read();
		return 0;
	}

	public int read(byte[] buffer) throws IOException
	{
		long count = getByteCount();
		if (count >= _maximumCount)
			return 0;
		else if ((count + buffer.length) <= _maximumCount)
			return super.read(buffer);
		return super.read(buffer, 0, (int)(_maximumCount - count));
	}

	public int read(byte[] buffer, int offset, int length) throws IOException
	{
		long count = getByteCount();
		if (count >= _maximumCount)
			return 0;
		else if ((count + length) <= _maximumCount)
			return super.read(buffer, offset, length);
		return super.read(buffer, offset, (int)(_maximumCount - count));
	}
}

