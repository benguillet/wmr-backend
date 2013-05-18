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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class SubmissionDatabase
{
	private static final String ADD_STMT =
		"INSERT INTO Submissions " +
		"(ID, User, Name, Timestamp, Test, Submitted, Completed) " +
		"VALUES (DEFAULT, ?, ?, NOW(), ?, DEFAULT, DEFAULT)";
	private static final String SET_INPUT_STMT =
		"UPDATE Submissions SET Input = ? WHERE ID = ?";
	private static final String SET_SUBMITTED_STMT =
		"UPDATE Submissions SET Submitted = TRUE WHERE ID = ?";
	private static final String SET_COMPLETED_STMT =
		"UPDATE Submissions SET Completed = TRUE WHERE ID = ?";
	private static final String SET_HADOOP_ID_STMT =
		"UPDATE Submissions SET Hadoop_ID = ? WHERE ID = ?";
	private static final String FIND_STMT =
		"SELECT * FROM Submissions WHERE ID = ?";
	private static final String QUOTA_STMT =
		"SELECT * FROM Submissions WHERE USER like ? AND TIMESTAMP >= ?";
	
	private static Connection _conn;
	private static PreparedStatement _addStmt;
	private static PreparedStatement _setInputStmt;
	private static PreparedStatement _setSubmittedStmt;
	private static PreparedStatement _setCompletedStmt;
	private static PreparedStatement _setHadoopIDStmt;
	private static PreparedStatement _findStmt;
	private static PreparedStatement _quotaStmt;
	
	public synchronized static void connect(Configuration conf)
		throws SQLException, ClassNotFoundException
	{
		if (_conn != null)
			throw new IllegalStateException(
					"Connection to the database has already been made");

		Class.forName("org.h2.Driver");
		_conn = DriverManager.getConnection(
				conf.get("wmr.db.url", "jdbc:h2:submissions"),
				conf.get("wmr.db.user", "wmrserver"),
				conf.get("wmr.db.password", ""));

		_addStmt = _conn.prepareStatement(ADD_STMT, Statement.RETURN_GENERATED_KEYS);
		_setInputStmt = _conn.prepareStatement(SET_INPUT_STMT);
		_setSubmittedStmt = _conn.prepareStatement(SET_SUBMITTED_STMT);
		_setCompletedStmt = _conn.prepareStatement(SET_COMPLETED_STMT);
		_setHadoopIDStmt = _conn.prepareStatement(SET_HADOOP_ID_STMT);
		_findStmt = _conn.prepareStatement(FIND_STMT);
		_quotaStmt = _conn.prepareStatement(QUOTA_STMT);
	}
	
	
	public static long add(String user, String name, boolean test)
		throws SQLException
	{
		synchronized (_addStmt)
		{
			_addStmt.setString(1, user);
			_addStmt.setString(2, name);
			_addStmt.setBoolean(3, test);
			_addStmt.executeUpdate();
			
			ResultSet keys = _addStmt.getGeneratedKeys();
			keys.next();
			return keys.getLong(1);
		}
	}

	public static boolean setInputPath(long id, Path inputPath)
		throws SQLException
	{
		synchronized (_setInputStmt)
		{
			_setInputStmt.setString(1, inputPath.toString());
			_setInputStmt.setLong(2, id);
			
			return (_setInputStmt.executeUpdate() != 0);
		}
	}

	public static boolean setSubmitted(long id)
		throws SQLException
	{
		synchronized (_setSubmittedStmt)
		{
			_setSubmittedStmt.setLong(1, id);
			
			return (_setSubmittedStmt.executeUpdate() != 0);
		}
	}
	
	public static boolean setCompleted(long id)
		throws SQLException
	{
		synchronized (_setCompletedStmt)
		{
			_setCompletedStmt.setLong(1, id);
			
			return (_setCompletedStmt.executeUpdate() != 0);
		}
	}

	public static boolean setHadoopID(long id, String hadoopID)
		throws SQLException
	{
		synchronized (_setHadoopIDStmt)
		{
			_setHadoopIDStmt.setString(1, hadoopID);
			_setHadoopIDStmt.setLong(2, id);
			
			return (_setHadoopIDStmt.executeUpdate() != 0);
		}
	}
	
	public static Submission find(long id)
		throws SQLException
	{
		synchronized (_findStmt)
		{
			_findStmt.setLong(1, id);
			return constructSubmission(_findStmt.executeQuery());
		}
	}
	
	/** Determines the number of jobs a user has submitted since a given time.
	 * If you wish to see this number for all users, pass in the "%" wildcard as the user string. */
	public static int jobsSince(String user, Timestamp time) 
		throws SQLException 
	{
		synchronized (_quotaStmt) {
			_quotaStmt.setString(1, user);
			_quotaStmt.setTimestamp(2, time);
			ResultSet res = _quotaStmt.executeQuery();
			int c = 0;
			//loop invariant: res.next() returns true if res has more than c elements
			while(res.next()) {
				c++;
			}
			return c;
		}
	}
	
	protected static Submission constructSubmission(ResultSet rs)
		throws SQLException
	{
		if (!rs.next())
			throw new SQLException("No results in result set.");
		
		return new Submission(rs);
	}
	
	protected static Set<Submission> constructAllSubmissions(ResultSet rs)
		throws SQLException
	{
		HashSet<Submission> set = new HashSet<Submission>();
		while (rs.next())
			set.add(new Submission(rs));
		return set;
	}
}
