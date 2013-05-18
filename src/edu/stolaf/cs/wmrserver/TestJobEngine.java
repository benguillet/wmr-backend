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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.commons.io.IOUtils;

import edu.stolaf.cs.wmrserver.db.Submission;
import edu.stolaf.cs.wmrserver.db.SubmissionDatabase;
import edu.stolaf.cs.wmrserver.thrift.*;
import edu.stolaf.cs.wmrserver.testjob.*;


public class TestJobEngine implements JobEngine
{
	private static final int POOL_SIZE = 1;
	
	/**
	 * A flag which specifies whether running test jobs is permitted.
	 */
	private boolean _allowTestJobs;
	private Configuration _conf;
	
	private ExecutorService _jobQueue;
	private HashMap<Long, Future<TestJobResult>> _jobs;
	
	TestJobEngine(Configuration conf)
	{
		_conf = conf;
		_allowTestJobs = _conf.getBoolean("wmr.tests.allow", false);

		_jobQueue = Executors.newFixedThreadPool(POOL_SIZE);
		_jobs = new HashMap<Long, Future<TestJobResult>>();
	}

	public void submit(JobRequest request, long submissionID, File mapperFile,
			File reducerFile, File packageDir, Path inputPath)
		throws ValidationException, NotFoundException,
		       CompilationException, InternalException, ForbiddenTestJobException
	{
		if(!_allowTestJobs) {
			throw new ForbiddenTestJobException("Test jobs are disabled on this system. Please submit this as a regular job.");
		}
		
		boolean numericSort = request.isNumericSort();
		
		TestJobTask job = new TestJobTask(_conf, submissionID, inputPath,
				mapperFile, reducerFile, packageDir, numericSort);
		submit(submissionID, job);
		
		try {
		    SubmissionDatabase.setSubmitted(submissionID);
		} catch (java.sql.SQLException ex) {
			throw JobServiceHandler.wrapException(
					"Could not update submission in database.", ex);
		}
	}

	public JobInfo getInfo(Submission submission)
		throws NotFoundException, InternalException
	{
		JobInfo info = new JobInfo();
		
		info.setNativeID(Long.toString(submission.getID()));
		info.setName(submission.getName());
		info.setTest(true);
		info.setInputPath(submission.getInput().toString());
		// Unfortunately, this could not be gotten to work with 
		// Hadoop 0.20.1
		/*Path homeDir = JobServiceHandler.getHome(_conf);
		info.setOutputPath(
			JobServiceHandler.relativePath(homeDir,
				FileOutputFormat.getOutputPath(_conf)).toString()); */
		info.setMapper(submission.getMapper());
		info.setReducer(submission.getReducer());
		
		return info;
	}

	public JobStatus getStatus(Submission submission)
			throws NotFoundException, InternalException
	{
		JobStatus status = new JobStatus();
		
		status.setInfo(getInfo(submission));
		
		if (!isComplete(submission.getID()))
		{
			// In progress
			
			status.setState(State.RUNNING);
			// Should be no way or need to set mapper or reducer progress.
			return status;
		}
		else
		{
			if (isKilled(submission.getID()))
			{
				// Killed
				
				status.setState(State.KILLED);
				return status;
			}
			else
			{
				// Succeeded or failed
				
				TestJobResult tjr;
				try
				{
					tjr = getResult(submission.getID());
				}
				catch (ExecutionException ex)
				{
					throw JobServiceHandler.wrapException(
							"A serious error prevented the test job from completing.",
							ex);
				}
				
				// Set map status
				PhaseStatus mapStatus = new PhaseStatus();
				mapStatus.setProgress(100);
				mapStatus.setCode(tjr.getMapResult().getExitCode());
				mapStatus.setOutput(readFile(tjr.getMapResult().getOutputFile()));
				mapStatus.setErrors(readFile(tjr.getMapResult().getErrorFile()));
				if (mapStatus.getCode() == 0)
					mapStatus.setState(State.SUCCESSFUL);
				else
					mapStatus.setState(State.FAILED);
				status.setMapStatus(mapStatus);
				
				// Set reduce status
				PhaseStatus reduceStatus = new PhaseStatus();
				reduceStatus.setCode(-1); // HACK
				if (tjr.getReduceResult() != null)
				{
					reduceStatus.setCode(tjr.getReduceResult().getExitCode());
					reduceStatus.setOutput(readFile(tjr.getReduceResult().getOutputFile()));
					reduceStatus.setErrors(readFile(tjr.getReduceResult().getErrorFile()));
					reduceStatus.setProgress(100);
					if (reduceStatus.getCode() == 0)
						reduceStatus.setState(State.SUCCESSFUL);
					else
						reduceStatus.setState(State.FAILED);
					status.setReduceStatus(reduceStatus);
				}
				
				// Set overall success
				if (mapStatus.getCode() == 0 && reduceStatus.getCode() == 0)
					status.setState(State.SUCCESSFUL);
				else
					status.setState(State.FAILED);
				
				return status;
			}
		}
	}

	public String getOutput(Submission submission, int page)
		throws IllegalJobStateException
	{
		throw new IllegalJobStateException(
				"This method does not read output for test jobs.");
	}

	public void kill(Submission submission) throws NotFoundException,
			IllegalJobStateException, InternalException
	{
		if (!kill(submission.getID()))
			throw new IllegalJobStateException(
					"Job was already completed before it could be killed");
	}


	private String readFile(File file) throws InternalException
	{
		if (file == null)
			return "";
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		FileInputStream in = null;
		try
		{
			in = new FileInputStream(file);
			IOUtils.copy(in, out);
		}
		catch (IOException ex)
		{
			throw JobServiceHandler.wrapException(
					"Could not read output/error file.", ex);
		}
		finally
		{
			IOUtils.closeQuietly(in);
		}
		return out.toString();
	}
	
	protected synchronized void submit(long submissionID, TestJobTask job)
	{
		Future<TestJobResult> jobFuture = _jobQueue.submit(job);
		_jobs.put(new Long(submissionID), jobFuture);
	}
	
	protected synchronized boolean kill(long submissionID)
		throws NotFoundException
	{
		return findJob(submissionID).cancel(true);
	}
	
	protected synchronized boolean isComplete(long submissionID)
		throws NotFoundException
	{
		return findJob(submissionID).isDone();
	}
	
	protected synchronized boolean isKilled(long submissionID)
		throws NotFoundException
	{
		return findJob(submissionID).isCancelled();
	}
	
	protected synchronized TestJobResult getResult(long submissionID)
		throws NotFoundException, ExecutionException, CancellationException
	{
		Future<TestJobResult> job = findJob(submissionID);
		while (true)
		{
			try
			{
				return job.get();
			}
			catch (InterruptedException ex)
			{
				// Swallow and try again
			}
		}
	}
	
	private synchronized Future<TestJobResult> findJob(long submissionID)
		throws NotFoundException
	{
		Future<TestJobResult> job = _jobs.get(submissionID);
		if (job == null)
			throw new NotFoundException(
					"Could not find test job with ID " +
					submissionID);
		return job;
	}
}
