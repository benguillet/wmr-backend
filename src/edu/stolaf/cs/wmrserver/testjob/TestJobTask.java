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

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.commons.exec.*;
import org.apache.commons.io.*;
import org.apache.commons.io.output.*;

import edu.stolaf.cs.wmrserver.AggregateInputStream;
import edu.stolaf.cs.wmrserver.JobServiceHandler;


/**
 * Runs jobs as test jobs, capturing output from both the mapper and reducer
 * executables. Since these jobs should only be run with small input, files are
 * cut off after a certain number of bytes.
 */
public class TestJobTask implements Callable<TestJobResult>
{
	public static final long EXECUTABLE_TIMEOUT = 30000; // 30 seconds
	
	Configuration _conf;
	File _tempDir;
	String _switchUserCommand;
	long _inputCap;
	
	long _id;
	Path _inputPath;
	File _mapperFile;
	File _reducerFile;
	File _packageDir;
	boolean _numericSort;
	
	
	public TestJobTask(Configuration conf, long id, Path inputPath,
			File mapperFile, File reducerFile, File packageDir, boolean numericSort)
	{
		_conf = conf;
		_tempDir = JobServiceHandler.getTempDir(conf);
		_inputCap = _conf.getLong("wmr.tests.input.cap", 1024); // 1KB
		_switchUserCommand = _conf.get("wmr.tests.su.cmd", null);
		
		_id = id;
		_inputPath = inputPath;
		_mapperFile = mapperFile;
		_reducerFile = reducerFile;
		_packageDir = packageDir;
		_numericSort = numericSort;
	}
	
	public TestJobResult call()
		throws IOException
	{
		// Create the result object
		TestJobResult result = new TestJobResult();
		
		
		// Map
		
		CappedInputStream mapInput = null;
		try
		{
			// List the input files and open a stream
			FileSystem fs = _inputPath.getFileSystem(_conf);
			FileStatus[] files = JobServiceHandler.listInputFiles(fs, _inputPath);
			AggregateInputStream aggregateInput = new AggregateInputStream(fs, files);
			mapInput = new CappedInputStream(aggregateInput, _inputCap);
			
			// Run the mapper
			result.setMapResult(
				runTransform(_id, _mapperFile, _packageDir, mapInput));
		}
		finally
		{
			IOUtils.closeQuietly(mapInput);
		}
		
		// Return if mapper failed or did not produce output
		if (result.getMapResult().getExitCode() != 0 ||
		    result.getMapResult().getOutputFile() == null)
			return result;
		
		
		// Sort
		// While this seems (and is) inefficient for computers, this is
		//actually probably the shortest way to write this code since
		// vanilla Java does not provide an equivalent of sort -n.
		// If you want to write it in Java, use java.util.TreeSet.
		
		File intermediateFile = null;
		FileOutputStream intermediateOutput = null;
		try
		{
			// Create and open temporary file for sorted intermediate output
			intermediateFile = File.createTempFile("job-" + Long.toString(_id),
					"-intermediate", _tempDir);
			intermediateOutput = new FileOutputStream(intermediateFile);
			
			
			// Run the sort
			
			CommandLine sortCommand = new CommandLine("sort");
			//sortCommand.addArgument("--field-separator=\t");
			if (_numericSort)
				sortCommand.addArgument("-n");
			sortCommand.addArgument(
				result.getMapResult().getOutputFile().getCanonicalPath(), false);
			DefaultExecutor exec = new DefaultExecutor();
			ExecuteWatchdog dog = new ExecuteWatchdog(EXECUTABLE_TIMEOUT);
			PumpStreamHandler pump = new PumpStreamHandler(intermediateOutput);
			exec.setWatchdog(dog);
			exec.setStreamHandler(pump);
			
			try
			{
				exec.execute(sortCommand);
			}
			catch (ExecuteException ex)
			{
				throw new IOException(
					"Sort process failed while running test jobs", ex);
			}
		}
		finally
		{
			IOUtils.closeQuietly(intermediateOutput);
		}
		
		
		// Reduce
		
		FileInputStream reduceInput = null;
		try
		{
			// Open the intermediate file for reading
			reduceInput = new FileInputStream(intermediateFile);
			
			// Run the reducer
			result.setReduceResult(
				runTransform(_id, _reducerFile, _packageDir, reduceInput));
		}
		finally
		{
			IOUtils.closeQuietly(reduceInput);
			
			// Delete intermediate file
			intermediateFile.delete();
		}
		
		return result;
	}
	
	protected TestJobResult.TransformResult runTransform(long id,
		File executable, File workingDir, InputStream input)
	throws IOException
	{
		// Create the result object
		TestJobResult.TransformResult result = new TestJobResult.TransformResult();
		
		CountingOutputStream output = null;
		CountingOutputStream error = null;
		try
		{
			// Create and open temporary file for standard output
			File outputFile = File.createTempFile("job-" + Long.toString(_id), "-output", _tempDir);
			output = new CountingOutputStream(new FileOutputStream(outputFile));
			
			// Create and open temporary file for standard error
			File errorFile = File.createTempFile("job-" + Long.toString(_id), "-error", _tempDir);
			error = new CountingOutputStream(new FileOutputStream(errorFile));
			
			// If executable is relative, try to resolve in working directory
			// (This emulates the behavior of Streaming)
			if (!executable.isAbsolute())
			{
				File resolvedExecutable = new File(workingDir, executable.toString());
				if (resolvedExecutable.isFile())
				{
					resolvedExecutable.setExecutable(true);
					executable = resolvedExecutable.getAbsoluteFile();
				}
			}
			
			
			// Run the transform
			
			CommandLine command;
			if (_switchUserCommand == null)
				command = new CommandLine(executable);
			else
			{
				command = CommandLine.parse(_switchUserCommand);
				HashMap<String, String> substitutionMap = new HashMap<String, String>();
				substitutionMap.put("cmd", executable.toString());
				command.setSubstitutionMap(substitutionMap);
			}

			DefaultExecutor executor = new DefaultExecutor();
			ExecuteWatchdog dog = new ExecuteWatchdog(EXECUTABLE_TIMEOUT);
			PumpStreamHandler pump = new PumpStreamHandler(output, error, input);
			executor.setWorkingDirectory(workingDir);
			executor.setWatchdog(dog);
			executor.setStreamHandler(pump);
			executor.setExitValues(null);
			
			int exitCode = executor.execute(command);
			
			result.setExitCode(exitCode);
			
			
			// Check whether it produced any output
			if (output.getByteCount() == 0)
			{
				output.close();
				outputFile.delete();
			}
			else
				result.setOutputFile(outputFile);
			
			// Check whether it produced any error output
			if (error.getByteCount() == 0)
			{
				error.close();
				errorFile.delete();
			}
			else
				result.setErrorFile(errorFile);
		}
		finally
		{
			IOUtils.closeQuietly(output);
			IOUtils.closeQuietly(error);
		}
		
		return result;
	}
}

