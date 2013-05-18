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

package edu.stolaf.cs.wmrserver.db;

import java.sql.*;

import org.apache.hadoop.fs.Path;

public class Submission
{	
	private long _id;
	private String _user;
	private String _name;
	private Timestamp _timestamp;
	private boolean _test;
	private boolean _submitted;
	private boolean _completed;
	private Path _input;
	
	// Hadoop jobs only
	private String _hadoopID;
	
	// Test jobs only
	private String _mapper;
	private String _reducer;
	
	
	public Submission(ResultSet rs) throws SQLException
	{
		_id = rs.getLong("ID");
		_user = rs.getString("User");
		_name = rs.getString("Name");
		_timestamp = rs.getTimestamp("Timestamp");
		_test = rs.getBoolean("Test");
		_submitted = rs.getBoolean("Submitted");
		_completed = rs.getBoolean("Completed");
		_input = null;
		if (rs.getString("Input") != null)
			_input = new Path(rs.getString("Input"));
		
		_hadoopID = rs.getString("Hadoop_ID");
		
		_mapper = rs.getString("Mapper");
		_reducer = rs.getString("Reducer");
	}
	
	
	public String getUser()
	{
		return _user;
	}
	
	public String getName()
	{
		return _name;
	}
	
	public Timestamp getTimestamp()
	{
		return _timestamp;
	}
	
	public boolean isTest()
	{
		return _test;
	}
	
	public boolean isSubmitted()
	{
		return _submitted;
	}
	
	public boolean isCompleted()
	{
		return _completed;
	}
	
	public Path getInput()
	{
		return _input;
	}
	
	public String getHadoopID()
	{
		return _hadoopID;
	}
	
	public String getMapper()
	{
		return _mapper;
	}
	
	public String getReducer()
	{
		return _reducer;
	}
	
	public long getID()
	{
		return _id;
	}


	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (_id ^ (_id >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Submission other = (Submission) obj;
		if (_id != other._id)
			return false;
		return true;
	}
	
	
}
