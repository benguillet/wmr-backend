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

package edu.stolaf.cs.wmrserver;

import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.fs.*;


public class AggregateInputStream extends InputStream
{
	FileSystem _fs;
	FileStatus[] _files;
	FSDataInputStream _currentStream;
	int _currentIndex;
	long _bytesRemaining;
	
	public AggregateInputStream(FileSystem fs, FileStatus[] files)
	{
		_fs = fs;
		_files = files;
		
		_currentStream = null;
		_currentIndex = -1;
		_bytesRemaining = -1;
	}
	
	public boolean markSupported()
	{
		return false;
	}
	
	public void mark(int readlimit)
	{
		throw new RuntimeException("Not implemented.");
	}
	
	public void reset()
	{
		throw new RuntimeException("Not implemented.");
	}
	
	public void close() throws IOException
	{
		if (_currentStream != null)
		{
			_currentStream.close();
			
			_currentStream = null;
			_currentIndex = -1;
			_bytesRemaining = -1;
		}
	}
	
	public int available() throws IOException
	{
		if (_currentStream != null)
			return _currentStream.available();
		else
			throw new IOException("Stream closed.");
	}
	
	public int read() throws IOException
	{
		while (_bytesRemaining <= 0)
			if (!next(true))
				return -1;
		
		_bytesRemaining--;
		return _currentStream.read();
	}
	
	public int read(byte[] b) throws IOException
	{
		return read(b, 0, b.length);
	}
	
	public int read(byte[] b, int off, int len) throws IOException
	{
		while (_bytesRemaining <= 0)
			if (!next(true))
				return -1;

		int bytesRead = _currentStream.read(b, off, len);
		_bytesRemaining -= bytesRead;
		return bytesRead;
	}
	
	public long skip(long n) throws IOException
	{
		long totalBytesSkipped = 0;
		
		while (_bytesRemaining <= 0)
			if (!next(false))
				return 0;
		
		do
		{
			if (n <= _bytesRemaining)
			{
				open();
				if (n > 0)
				{
					long bytesSkipped = _currentStream.skip(n);
					_bytesRemaining -= bytesSkipped;
					totalBytesSkipped += bytesSkipped;
				}
				return totalBytesSkipped;
			}
			else
			{
				totalBytesSkipped += _bytesRemaining;
				n -= _bytesRemaining;
			}
		} while (next(false));
		
		return totalBytesSkipped;
	}
	
	
	private boolean next(boolean open) throws IOException
	{
		if (_currentStream != null)
		{
			_currentStream.close();
			_currentStream = null;
			_bytesRemaining = -1;
		}
		
		if (_currentIndex == _files.length - 1)
			return false;
		
		_currentIndex++;
		_bytesRemaining = _files[_currentIndex].getLen();
		if (open)
			open();
		return true;
	}
	
	private void open() throws IOException
	{
		if (_currentStream == null)
			_currentStream = _fs.open(_files[_currentIndex].getPath());
	}
}
