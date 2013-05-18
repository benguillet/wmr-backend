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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.lang.reflect.Method;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner;
import org.apache.hadoop.mapred.lib.KeyFieldBasedComparator;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import edu.stolaf.cs.wmrserver.db.Submission;
import edu.stolaf.cs.wmrserver.db.SubmissionDatabase;
import edu.stolaf.cs.wmrserver.streaming.StreamJob;
import edu.stolaf.cs.wmrserver.thrift.*;
import edu.stolaf.cs.wmrserver.thrift.JobStatus;


public class HadoopEngine implements JobEngine
{
	/** A JobConf key which will specify the job's originating user */
	public static final String CONF_USER = "wmr.submit.user";
	/** A JobConf key which will specify the language the job was written in. */
	public static final String CONF_LANGUAGE = "wmr.submit.language";
	public static final String CONF_MAPPER = "wmr.mapper";
	public static final String CONF_REDUCER = "wmr.reducer";
	public static final String CONF_NUMERIC = "wmr.numeric.sort";

	
	/**
	 * The directory on the DFS which will contain all WebMapReduce-related files.
	 */
	private Path _homeDir;
	/**
	 * The local directory which will contain mapper and reducer scripts,
	 * test job output, etc.
	 */
	private File _tempDir;
	
	public HadoopEngine(Configuration conf) throws IOException
	{
		_homeDir = JobServiceHandler.getHome(conf);
		_tempDir = JobServiceHandler.getTempDir(conf);
	}
	
	public void submit(JobRequest request, long submissionID, File mapperFile,
			File reducerFile, File packageDir, Path inputPath)
		throws ValidationException, NotFoundException,
		       CompilationException, InternalException
	{
		// Generate job output path
		Path outputDir = new Path(_homeDir, "out");
		Path outputPath;
		try
		{
			FileSystem fs = outputDir.getFileSystem(new Configuration());
			outputPath = JobServiceHandler.getNonexistantPath(
					outputDir, request.getName(), fs);
		}
		catch (IOException ex)
		{
			throw JobServiceHandler.wrapException("Could not construct output path.", ex);
		}
		
		JobConf conf = new JobConf();
		conf.setJobName(request.getName());
		
		// Set mapper and number of tasks if specified
		StreamJob.setStreamMapper(conf, mapperFile.toString());
		if (request.isSetMapTasks())
			conf.setNumMapTasks(request.getMapTasks());
		
		// Set reducer and number of tasks if specified
		StreamJob.setStreamReducer(conf, reducerFile.toString());
		if (request.isSetReduceTasks())
			conf.setNumReduceTasks(request.getReduceTasks());
		
		// Create and set job JAR, including necessary files
		ArrayList<String> jarFiles = new ArrayList<String>();
		jarFiles.add(packageDir.toString());
		String jarPath;
		try
		{
			jarPath = StreamJob.createJobJar(conf, jarFiles, _tempDir);
		}
		catch (IOException ex)
		{
			throw JobServiceHandler.wrapException("Could not create job jar.", ex);
		}
		if (jarPath != null)
			conf.setJar(jarPath);
		
		// TODO: This is a hack. Rewrite streaming to use DistributedCache.
		//conf.setPattern("mapreduce.job.jar.unpack.pattern",
		//		        Pattern.compile(".*"));
		
		// Set I/O formats and paths
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		
		// Use numeric sort if appropriate
		conf.setBoolean(CONF_NUMERIC, request.isNumericSort());
		if (request.isNumericSort())
		{
			conf.setOutputKeyComparatorClass(KeyFieldBasedComparator.class);
			conf.setPartitionerClass(KeyFieldBasedPartitioner.class);
			conf.setKeyFieldComparatorOptions("-n");
			conf.setKeyFieldPartitionerOptions("-n");
		}
		
		// Set other job information
		conf.set(CONF_USER, request.getUser());
		conf.set(CONF_LANGUAGE, request.getLanguage());
		conf.set(CONF_MAPPER, request.getMapper());
		conf.set(CONF_REDUCER, request.getReducer());
		
		
		// Attempt to submit the job
		
		RunningJob job;
		try
		{
			JobClient client = new JobClient(new JobConf());
			job = client.submitJob(conf);
		}
		catch (IOException ex)
		{
			throw JobServiceHandler.wrapException(
				"There was a serious error while attempting to submit the job.",
				ex);
		}
		
		try
		{
			SubmissionDatabase.setSubmitted(submissionID);
			SubmissionDatabase.setHadoopID(submissionID, job.getID().toString());
		}
		catch (SQLException ex)
		{
			throw JobServiceHandler.wrapException(
					"Could not update submission in database.", ex);
		}
	}

	public JobInfo getInfo(Submission submission, RunningJob job, JobConf conf) throws NotFoundException,
			InternalException
	{
		
		JobInfo info = new JobInfo();
		
		info.setNativeID(submission.getHadoopID());
		info.setName(job.getJobName());
		info.setTest(false);
		
		if (conf == null)
			// Can't proceed any further if configuration is unavailable
			return info;

		info.setRequestedMapTasks(conf.getNumMapTasks());
		info.setRequestedReduceTasks(conf.getNumReduceTasks());
		info.setMapper(conf.get(CONF_MAPPER));
		info.setReducer(conf.get(CONF_REDUCER));
    info.setNumericSort(conf.getBoolean(CONF_NUMERIC, false));
		info.setInputPath(
			JobServiceHandler.relativizePath(_homeDir,
				FileInputFormat.getInputPaths(conf)[0]).toString());
		info.setOutputPath(
			JobServiceHandler.relativizePath(_homeDir,
				FileOutputFormat.getOutputPath(conf)).toString());
		
		return info;
	}
	
	private static class TaskLog
	{
		private String trackerHttp;
		private TaskAttemptID attemptID;
		
		public TaskLog(String trackerHttp, TaskAttemptID attemptID)
		{
			this.trackerHttp = trackerHttp;
			this.attemptID = attemptID;
		}
		
		public String getURL(boolean error)
		{
			// NOTE: This URL format could change between versions of Hadoop
			String filter = error ? "stderr" : "stdout";
			return trackerHttp + "/tasklog?plaintext=true" +
				"&attemptid=" + attemptID.toString() + 
				"&filter=" + filter;
		}
		
		public String fetch(boolean error)
			throws MalformedURLException, IOException
		{
			InputStream logStream = new URL(getURL(error)).openStream();
			return IOUtils.toString(logStream);
		}
	}
	
	private static class Pair<T1, T2>
	{
		public T1 first;
		public T2 second;
		
		public Pair(T1 first, T2 second)
		{
			this.first = first;
			this.second = second;
		}
	}

	public JobStatus getStatus(Submission submission)
			throws NotFoundException, InternalException
	{
		RunningJob job = getJob(submission);
		JobConf conf = loadJobConfiguration(job);
		
		JobStatus status = new JobStatus();
		status.setInfo(getInfo(submission, job, conf));
		
		try
		{
			JobClient client = new JobClient(new JobConf());
			
			// Get job state
			// Thanks to the mentally handicapped switch statement, we have
			// to use a chain of ifs. Fuck Java.
			int jobState = job.getJobState();
			if (jobState == org.apache.hadoop.mapred.JobStatus.FAILED)
				status.setState(State.FAILED);
			else if (jobState == org.apache.hadoop.mapred.JobStatus.SUCCEEDED)
				status.setState(State.SUCCESSFUL);
			else if (jobState == org.apache.hadoop.mapred.JobStatus.KILLED)
				status.setState(State.KILLED);
			else if (jobState == org.apache.hadoop.mapred.JobStatus.RUNNING)
				status.setState(State.RUNNING);
			else
				status.setState(State.PREP);
			
			// Get task counts
			TaskReport[] mapTaskReports = client.getMapTaskReports(job.getID());
			TaskReport[] reduceTaskReports = client.getReduceTaskReports(job.getID());
			
			// Get failed task logs
			TaskCompletionEvent[] events = job.getTaskCompletionEvents(0);
			Pair<ArrayList<TaskLog>, ArrayList<TaskLog>> failures;
			if (events != null)
				failures = getLogsFromCompletionEvents(events);
			else
				failures = getLogsFromHistory(job, new Configuration());
			ArrayList<TaskLog> mapFailures = failures.first;
			ArrayList<TaskLog> reduceFailures = failures.second;
			
			// Get other mapper info
			PhaseStatus mapStatus = new PhaseStatus();
			mapStatus.setProgress(job.mapProgress() * 100);
			if (!mapFailures.isEmpty())
				mapStatus.setErrors(getMeaningfulTaskLog(mapFailures));
			if (mapTaskReports != null)
				mapStatus.setTotalTasks(mapTaskReports.length);
			// TODO: Handle the state in a sane way
			mapStatus.setState(status.getState());
			status.setMapStatus(mapStatus);
			
			// Get other reducer info
			PhaseStatus reduceStatus = new PhaseStatus();
			reduceStatus.setProgress(job.reduceProgress() * 100);
			if (!reduceFailures.isEmpty())
				reduceStatus.setErrors(getMeaningfulTaskLog(reduceFailures));
			reduceStatus.setState(status.getState());
			if (reduceTaskReports != null)
				reduceStatus.setTotalTasks(reduceTaskReports.length);
			if (conf != null)
				reduceStatus.setOutputPath(
						FileOutputFormat.getOutputPath(conf).toString());
			status.setReduceStatus(reduceStatus);
		}
		catch (Exception ex)
		{
			throw JobServiceHandler.wrapException("Could not get job info.", ex);
		}
		
		return status;
	}
	
	private Pair<ArrayList<TaskLog>, ArrayList<TaskLog>>
		getLogsFromCompletionEvents(TaskCompletionEvent[] events)
	{
		ArrayList<TaskLog> mapFailures = new ArrayList<TaskLog>();
		ArrayList<TaskLog> reduceFailures = new ArrayList<TaskLog>();
		for (TaskCompletionEvent event : events)
		{
			if (event.getTaskStatus() != TaskCompletionEvent.Status.SUCCEEDED)
			{
				TaskLog log = new TaskLog(event.getTaskTrackerHttp(),
																	event.getTaskAttemptId());
				if (event.isMapTask())
					mapFailures.add(log);
				else
					reduceFailures.add(log);
			}
		}
		
		return new Pair<ArrayList<TaskLog>, ArrayList<TaskLog>>(
			mapFailures, reduceFailures);
	}
	
	private Pair<ArrayList<TaskLog>, ArrayList<TaskLog>>
		getLogsFromHistory(RunningJob job, Configuration conf)
		throws InternalException
	{
		// NOTE: Hadoop 0.21 only!
		
		throw new InternalException("Not implemented for Hadoop 0.20");
		
		/*
		JobHistoryParser.JobInfo jobInfo = null;
		try
		{
			Class jhpClass = Class.forName(
					"org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser");
			Class jhpJobInfoClass = Class.forName(
					"org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo");
			Class jhpTaskInfoClass = Class.forName(
					"org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo");
			Class jhpTaskAttemptInfoClass = Class.forName(
					"org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo");
			
			//JobHistoryParser.JobInfo jobInfo;
			Object jobInfo = null;
			try
			{
				Path historyPath = new Path(job.getHistoryUrl());
				FileSystem fs = historyPath.getFileSystem(conf);
				
				//JobHistoryParser parser;
				Object parser = jhpClass.getConstructor(FileSystem.class, Path.class).
					newInstance(fs, historyPath);
				jobInfo = jhpClass.getMethod("parse").invoke(parser);
			}
			catch (IOException ex)
			{
				JobServiceHandler.wrapException("Could not retrieve job history file.", ex);
			}
			
			// TODO: Should only failed *tasks* be inspected? i.e., if a failure is
			//  recovered from, should it be reported?
			ArrayList<TaskLog> mapFailures = new ArrayList<TaskLog>();
			ArrayList<TaskLog> reduceFailures = new ArrayList<TaskLog>();
			
			Map tasks = jhpJobInfoClass.getMethod("getAllTasks").invoke(jobInfo);
			//JobHistoryParser.TaskInfo taskInfo;
			for (Object taskInfo : tasks.values())
			{
				Map attempts = jhpTaskInfoClass.getMethod("getAllTaskAttempts").invoke(taskInfo);
				//JobHistoryParser.TaskAttemptInfo attemptInfo;
				for (Object attemptInfo : attempts.values())
				{
					String taskStatus = jhpTaskAttemptInfoClass.getMethod("getTaskStatus").invoke(attemptInfo);
					if (!taskStatus.equals("SUCCEEDED"))
					{
						String hostname = jhpTaskAttemptInfoClass.getMethod("getHostname").invoke(attemptInfo);
						int port = jhpTaskAttemptInfoClass.getMethod("getHttpPort").invoke(attemptInfo);
						TaskAttemptID attemptID = jhpTaskAttemptInfoClass.getMethod("getAttemptId").invoke(attemptInfo);
						TaskLog log = new TaskLog(hostname + ":" + port, attemptID);
						
						
						Enum 
						switch (attemptInfo.getTaskType())
						{
							case MAP:
								mapFailures.add(log);
							case REDUCE:
								reduceFailures.add(log);
							default:
								// TODO: Figure out how to handle this
								throw new InternalException("Setup or cleanup task failed.");
						}
					}
				}
			}
		}
		catch (ClassNotFoundException ex)
		{
		}
		
		return new Pair<ArrayList<TaskLog>, ArrayList<TaskLog>>(
			mapFailures, reduceFailures);
		*/
	}
	
	public void kill(Submission submission) throws NotFoundException,
			IllegalJobStateException, InternalException
	{
		RunningJob job = getJob(submission);
		
		// Attempt to kill job
		try
		{
			if (job.isComplete())
				throw new IllegalJobStateException(
				"Job was already complete before it could be killed.");
			
			job.killJob();
		}
		catch (IOException ex)
		{
			throw JobServiceHandler.wrapException("Job could not be killed.", ex);
		}
	}
	
	private RunningJob getJob(Submission submission) throws NotFoundException, InternalException
	{
		if (!submission.isSubmitted())
			throw new NotFoundException(
					"Job with specified ID exists, but was not submitted to Hadoop.");
		
		JobID requestedID;
		try
		{
			requestedID = JobID.forName(submission.getHadoopID());
		}
		catch (IllegalArgumentException ex)
		{
			throw JobServiceHandler.wrapException("Hadoop Job ID was malformed.", ex);
		}
		
		RunningJob job;
		try
		{
			JobClient client = new JobClient(new JobConf());
			job = client.getJob(requestedID);
		}
		catch (Exception ex)
		{
			throw JobServiceHandler.wrapException("Could not get Hadoop job.", ex);
		}
		
		if (job == null)
			throw new NotFoundException("The job specifed by ID \"" +
					requestedID.toString() + "\" was not found.");
		
		return job;
	}
	
	private JobConf loadJobConfiguration(RunningJob job) throws InternalException
	{	
		// Try normal job file
		try
		{
			JobConf conf = new JobConf();
			Path jobFile = new Path(job.getJobFile());
			FileSystem fs = jobFile.getFileSystem(new Configuration());
			conf.addResource(fs.open(jobFile));
			
			return conf;
		}
		catch (IOException ex)
		{
		}
		catch (IllegalArgumentException ex)
		{
		}
		
		// Hadoop 0.20 only
		return new JobConf(org.apache.hadoop.mapred.JobTracker.getLocalJobFilePath(job.getID()));
		
		/*
		// Try to retrieve configuration from history
		// Hadoop 0.21 only!
		try
		{
			Method m = JobTracker.class.getMethod("getLocalJobFilePath", JobID.class);
			String jobFile = m.invoke(null, job.getID());
			return new JobConf(jobFile);
		}
		catch (NoSuchMethodException ex)
		{
		}
		catch (SecurityException ex)
		{
		}
		
		// Try to retrieve configuration from history (0.21 only)
		try
		{
			Method getHistoryUrl = job.getClass().getMethod("getHistoryUrl");
			
			Path historyPath = new Path(getHistoryUrl.invoke(job));
			Path historyDir = historyPath.getParent();
			
			Class jobHistoryClass = Class.forName(
					"org.apache.hadoop.mapreduce.jobhistory.JobHistory");
			Method getConfFile = jobHistoryClass.getMethod(
					"getConfFile", Path.class, JobID.class);
			
			Path jobFile = getConfFile.invoke(null, historyDir, job.getID());
			
			return new JobConf(jobFile);
		}
		catch (IOException ex)
		{
		}
		catch (IllegalArgumentException ex)
		{
			// Thrown for empty string in Path
			// This should only be temporary
		}
		
		return null;
		*/
	}
	
	/**
	 * Attempts to retrieve the first "meaningful" task log from the set given.
	 * Currently returns the first non-blank log.
	 * 
	 * @param events  The set of task logs.
	 * @return A complete task attempt log
	 */
	private String getMeaningfulTaskLog(ArrayList<TaskLog> logs)
	{
		// Find first non-blank log
		for (TaskLog log : logs)
		{
			try
			{
				String logContents = log.fetch(true);
				if (!StringUtils.isBlank(logContents))
					return logContents;
			}
			catch (Exception ex)
			{
				return "[Error retrieving task log at " + log.getURL(true) + "]";
			}
		}
		
		// No non-blank logs found
		return "";
	}
}
