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

import edu.stolaf.cs.wmrserver.db.Submission;
import edu.stolaf.cs.wmrserver.db.SubmissionDatabase;
import edu.stolaf.cs.wmrserver.thrift.*;

import java.io.*;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.util.Date;
import java.sql.Timestamp;


public class JobServiceHandler implements JobService.Iface
{
	public static final String LANG_CONF_FILE = "languages.conf";
	
	/**
	 * The directory on the DFS which will contain all WebMapReduce-related files.
	 */
	private Path _homeDir;
	/**
	 * The local directory which will contain mapper and reducer scripts,
	 * test job output, etc.
	 */
	private File _tempDir;
	/**
	 * The directory containing language support files, including the 
	 * configuration file "languages.conf".
	 */
	private File _langSupportDir;
	/**
	 * A flag specifying whether job input must be contained within the
	 * WMR home directory on the DFS.
	 */
	private boolean _enforceInputContainment;
	/**
	 * A flag specifying whether to disallow reads from the local filesystem.
	 * Note that this does not prevent users' jobs from doing so--it only blocks
	 * job input and read requests from the local filesystem.
	 */
	private boolean _disallowLocalInput;
	/**
	 * The size of a page of output in bytes.
	 */
	private int _outputPageSize;
	/** Whether the quota system is enabled. If so, a user will only be able to submit a certain number of jobs (default 20) in a given period of time (default 10 minutes) */
	private boolean _quotaEnabled;
	/** A number specifying the number of jobs that a user may have in a given period,
	 * specified by _quotaDuration. */
	private int _quotaAttempts;
	/** A the number of minutes during which a user may submitted only a limitted number of 
	 * jobs, if the quota is enabled. */
	private int _quotaDuration;
	
	/**
	 * The configuration of supported languages for mapper & reducer scripts. Use {@link
	 * #getLanguageConf()} instead of accessing directly.
	 */
	private HierarchicalConfiguration _languageConf;
	
	private HadoopEngine _hadoopEngine;
	private TestJobEngine _testJobEngine;
	
	
	public JobServiceHandler(Configuration conf) throws IOException
	{
		_homeDir = getHome(conf);
		_tempDir = getTempDir(conf);
		_langSupportDir = new File(
			conf.get("wmr.lang.support.dir", "lang-support"));
		_enforceInputContainment =
			conf.getBoolean("wmr.input.containment.enforce", false);
		_disallowLocalInput =
			conf.getBoolean("wmr.input.disallow.local", true);
		_outputPageSize = getOutputPageSize(conf);
		_quotaEnabled = conf.getBoolean("wmr.quota.enable", true) &&
			conf.getBoolean("wmr.quota.user.enable", true);
		_quotaAttempts = 
			conf.getInt("wmr.quota.user.attempts", 20);
		_quotaDuration = 
			conf.getInt("wmr.quota.user.duration", 10);
		
		// Resolve relative lang support dir
		if (!_langSupportDir.isAbsolute())
			_langSupportDir = new File(
				System.getProperty("wmr.home.dir"),
				_langSupportDir.toString());
		
		// Load language configuration
		File wmrConfFile = new File(_langSupportDir, LANG_CONF_FILE);
		if (!wmrConfFile.exists())
			throw new IOException(
				"Language configuration could not be found: " +
				wmrConfFile.toString());
		try
		{
			_languageConf = new HierarchicalINIConfiguration(wmrConfFile);
		}
		catch (ConfigurationException ex)
		{
			throw new IOException(
				"The language configuration could not be loaded.", ex);
		}
		
		_hadoopEngine = new HadoopEngine(conf);
		_testJobEngine = new TestJobEngine(conf);
	}
	
	
	public long submit(JobRequest request)
		throws ValidationException, NotFoundException, CompilationException,
		       InternalException, PermissionException, QuotaException,
		       ForbiddenTestJobException
	{
		
		// Basic validation
		
		if (StringUtils.isBlank(request.getMapper()))
			throw new ValidationException("Mapper was not specified.", request.getMapper());
		if (StringUtils.isBlank(request.getReducer()))
			throw new ValidationException("Reducer was not specified.", request.getReducer());
		if (request.isSetMapTasks() && request.getMapTasks() < 1)
			throw new ValidationException("Jobs must have at least one map task.",
					Integer.toString(request.getMapTasks()));
		if (request.isSetReduceTasks() && request.getReduceTasks() < 1)
			throw new ValidationException("Jobs must have at least one reduce task.",
					Integer.toString(request.getReduceTasks()));
		
		if (StringUtils.isBlank(request.getName()))
			request.setName("job");
		else if (!request.getName().matches("[ \\S]*"))
			throw new ValidationException(
				"Job name can only contain printing characters and spaces.", request.getName());
		
		String inputSpecifier = request.getInput();
		if (StringUtils.isBlank(inputSpecifier))
			throw new ValidationException("No input was specified.", inputSpecifier);
		
		//We might also do total quotas, but not necessarily
		//Determine if the user is over his quota

		int recentJobs=-1;//the number of jobs should always be positive, so this should be safe
		try
		{
			Date now = new Date();
			//60,000 is a minute in milliseconds, the units of Date.
			long quotaBegan = now.getTime() - _quotaDuration * 60000;
			recentJobs = SubmissionDatabase.jobsSince(request.getUser(), new Timestamp(quotaBegan));
		}
	       	catch (SQLException ex) {
			throw wrapException("Could not access the database of jobs to check whether you have exceeded your quota.", ex);
		}

		if(recentJobs >= _quotaAttempts) {
			throw new QuotaException("You have exceeded your job quota. Please resubmit this job in a few minutes.", _quotaAttempts, _quotaDuration);
		}

		// Log submission attempt
		
		long submissionID;
		try
		{
			submissionID = SubmissionDatabase.add(request.getUser(),
					request.getName(), request.isTest());
		}
		catch (SQLException ex)
		{
			throw wrapException("Could not add submission to database.", ex);
		}
		
		
		// Get input path

		Path inputPath;
		FileSystem fs;
		try
		{
			inputPath = resolvePath(inputSpecifier);
			fs = inputPath.getFileSystem(new Configuration());
			checkPath(fs, inputPath);
		}
		catch (IOException ex)
		{
			throw wrapException("Could not check validity of input path.", ex);
		}
		
		try
		{
			SubmissionDatabase.setInputPath(submissionID, inputPath);
		}
		catch (SQLException ex)
		{
			throw wrapException("Could not update submission in database", ex);
		}
		
		
		// Process & write the mapper and reducer and get their locations
		
		TransformProcessor processor = new TransformProcessor(
			_languageConf, _langSupportDir, _tempDir, request.getName());
		File packageDir, mapperFile, reducerFile;
		try
		{
			packageDir = processor.getPackageDir();
			mapperFile = processor.prepareMapperPackage(
					request.getMapper(), request.getLanguage());
			reducerFile = processor.prepareReducerPackage(
					request.getReducer(), request.getLanguage());
		}
		catch (IOException ex)
		{
			throw wrapException("Could not package mapper/reducer.", ex);
		}
		
		
		// Delegate handling to appropriate job engine
		
		if (request.isTest())
			_testJobEngine.submit(request, submissionID, mapperFile,
					reducerFile, packageDir, inputPath);
		else
			_hadoopEngine.submit(request, submissionID, mapperFile,
					reducerFile, packageDir, inputPath);
		
		return submissionID;
	}
	
	public String storeDataset(String name, String data) throws InternalException
	{
		// Use default name if not given (this will be made unique later)
		if (StringUtils.isBlank(name))
			name = "dataset";
		
		// Store on the DFS
		Path dataDir = new Path(_homeDir, "data");
		Path inputPath;
		FSDataOutputStream saveStream = null;
		try
		{
			FileSystem fs = dataDir.getFileSystem(new Configuration());
			inputPath = getNonexistantPath(dataDir, name, fs);
			
			String processedData = data.replace("\r\n", "\n");
			
			saveStream = fs.create(inputPath, true);
			IOUtils.write(processedData, saveStream);
		}
		catch (IOException ex)
		{
			throw wrapException("Job input could not be written to the DFS.", ex);
		}
		finally
		{
			if (saveStream != null)
				IOUtils.closeQuietly(saveStream);
		}
		
		return relativizePath(_homeDir, inputPath).toString();
	}
	
	public DataPage readDataPage(String pathString, int page)
			throws NotFoundException, InternalException, PermissionException
	{
		FileSystem fs;
		FileStatus[] partFiles;
		try
		{
			Path path = resolvePath(pathString);
			fs = path.getFileSystem(new Configuration());
			
			checkPath(fs, path);
			
			partFiles = listInputFiles(fs, path);
		}
		catch (IOException ex)
		{
			throw JobServiceHandler.wrapException("Could not list files.", ex);
		}

		long totalSize = 0;
		for (FileStatus part : partFiles)
			totalSize += part.getLen();
		if (totalSize == 0)
		{
			// Return empty output
			DataPage ret = new DataPage();
			ret.setTotalPages(0);
			ret.setData(null);
			return ret;
		}
		
		int totalPages = (int)Math.ceil((double)totalSize / _outputPageSize);
		if (page > totalPages)
			throw new NotFoundException(
				"Specified page \"" + Integer.toString(page) + "\" is beyond the " +
				"number of pages in the data.");
		
		
		// Read data page
		String data = null;
		AggregateInputStream input = null;
		try
		{
			int totalBytesRead = 0;
			byte[] buffer = new byte[_outputPageSize];
			input = new AggregateInputStream(fs, partFiles);
			
			// Seek to page
			input.skip(_outputPageSize * (page - 1));
			
			// Read as much as possible into buffer
			while (totalBytesRead < _outputPageSize)
			{
				int bytesRead = input.read(
					buffer, totalBytesRead, _outputPageSize - totalBytesRead);
				
				if (bytesRead == -1)
					break;
				else
					totalBytesRead += bytesRead;
			}
			
			// Convert to string
			data = new String(buffer, 0, totalBytesRead);
		}
		catch (IOException ex)
		{
			throw JobServiceHandler.wrapException("Could not copy data page.", ex);
		}
		finally
		{
			if (input != null)
				IOUtils.closeQuietly(input);
		}
		
		DataPage ret = new DataPage();
		ret.setTotalPages(totalPages);
		ret.setData(data);
		return ret;
	}


	public JobStatus getStatus(long id)
		throws NotFoundException, InternalException
	{
		Submission submission = getSubmission(id);
		if (submission.isTest())
			return _testJobEngine.getStatus(submission);
		else
			return _hadoopEngine.getStatus(submission);
	}


	public void kill(long id)
		throws NotFoundException, IllegalJobStateException, InternalException
	{
		Submission submission = getSubmission(id);
		if (submission.isTest())
			_testJobEngine.kill(submission);
		else
			_hadoopEngine.kill(submission);
	}
	
	
	public Submission getSubmission(long id)
		throws NotFoundException
	{
		Submission submission;
		try
		{
			submission = SubmissionDatabase.find(id);
		}
		catch (SQLException ex)
		{
			submission = null;
		}
		
		if (submission == null)
			throw new NotFoundException("Could not find job with specified ID: " + id);
		
		return submission;
	}
	
	public static InternalException wrapException(String message, Throwable cause)
	{
		InternalException ex = new InternalException(message);
		
		// Serialize cause chain
		ArrayList<CausingException> causeChain = new ArrayList<CausingException>();
		while (cause != null)
		{
			CausingException causeStruct = new CausingException();
			
			causeStruct.setMessage(cause.getMessage());
			causeStruct.setType(cause.getClass().getName());
			
			// Store stack trace as string
			StringWriter stackTraceWriter = new StringWriter();
			cause.printStackTrace(new PrintWriter(stackTraceWriter));
			causeStruct.setStackTrace(stackTraceWriter.toString());
			
			causeChain.add(causeStruct);
			
			// Move to next in chain and loop
			cause = cause.getCause();
		}
		ex.setCauses(causeChain);
		
		return ex;
	}
	
	public static Path getHome(Configuration conf) throws IOException
	{
		String wmrHomeString = conf.get("wmr.dfs.home");
		if (!StringUtils.isEmpty(wmrHomeString))
			return new Path(wmrHomeString);
		else
			return FileSystem.get(conf).getWorkingDirectory();
	}
	
	public static File getTempDir(Configuration conf)
	{
		return new File(conf.get("wmr.temp.dir", System.getProperty("java.io.tmpdir")));
	}
	
	public static int getOutputPageSize(Configuration conf)
	{
		return conf.getInt("wmr.status.output.pagesize", 0x80000 /* 512 K */);
	}
	
	private Path resolvePath(String pathString)
	{
		Path path = new Path(pathString);
		if (!path.isAbsolute())
			path = new Path(_homeDir, pathString);
		return path;
	}
	
	private void checkPath(FileSystem fs, Path path)
		throws PermissionException, NotFoundException, IOException
	{	
		if (_disallowLocalInput)
		{
		        // If we update to Hadoop 1.0, we should use the canonical URI which is definitely unique to each file system. However, the normal one should be, too.
			if (fs.getUri().equals(
				    FileSystem.getLocal(new Configuration()).getUri()))
			{
				throw new PermissionException(
					"Not allowed to read from the local file system.");
			}
		}
		
		if (!fs.exists(path))
			throw new NotFoundException("Input path does not exist: " + path.toString());
		
		if (_enforceInputContainment)
		{
			// Check that path is inside home directory
			Path relativePath = relativizePath(_homeDir, path);
			if (relativePath.isAbsolute()); // Has authority or begins with "/"
			throw new PermissionException("Not allowed to read outside the " +
					"WebMapReduce home directory (" + _homeDir.toString() +
					"). Please specify a relative path.");
		}
	}
	
	/**
	 * Relativize the given path with respect to the WMR home directory
	 */
	public static Path relativizePath(Path parentPath, Path childPath)
	{
		URI relative = parentPath.toUri().relativize(childPath.toUri());
		return new Path(
			relative.getScheme(), relative.getAuthority(), relative.getPath());
	}
	
	public static Path getNonexistantPath(Path parentDir, String name, FileSystem fs)
		throws IOException
	{
		Path requestedPath = new Path(parentDir, name);
		Path path = requestedPath;
		int serial = 1;
		while (fs.exists(path))
		{
			path = requestedPath.suffix("-" + serial);
			serial++;
		}
		return path;
	}
	
	public static FileStatus[] listInputFiles(FileSystem fs, Path path)
		throws IOException
	{
		if (!fs.isDirectory(path))
			return new FileStatus[] { fs.getFileStatus(path) };
		else
		{
			// Get all files in directory that are not directories or hidden files
			
			final FileSystem fsFinal = fs;
			PathFilter filter = new PathFilter() {
				public boolean accept(Path p) {
					try
					{
						return !(fsFinal.isDirectory(p) ||
										 p.getName().startsWith(".") ||
										 p.getName().startsWith("_"));
					}
					catch (IOException ex)
					{
						throw new RuntimeException("Error filtering files.", ex);
					}
				}
			};
			
			return fs.listStatus(path, filter);
		}
	}
}
