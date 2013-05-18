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

import java.io.*;
import java.net.URI;
import java.util.Hashtable;
import java.util.regex.Pattern;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.exec.*;

import edu.stolaf.cs.wmrserver.thrift.CompilationException;
import edu.stolaf.cs.wmrserver.thrift.ValidationException;

/**
 * Provides methods to prepare and package transform (map or reduce) executables
 * from users' code. Applies library code, compiles, and generates scripts as
 * necessary for the desired language.
 */
public class TransformProcessor
{
	private HierarchicalConfiguration m_wmrConfig;
	private File m_tempDir;
	private File m_jobTempDir;
	private File m_languageConfDir;
	private String m_jobName;
	
	/** 
	 * Constructs a new TransformProcessor that will reference the given
	 * language configuration and use the given temp directory.
	 *
	 * @param wmrConfig   The language configuration for WebMapReduce
	 * @param langConfDir The directory containing language support files
	 * @param tempDir     The directory in which to store processed files
	 * @param jobName     The name of the job, used in the script's filename
	 */
	public TransformProcessor(HierarchicalConfiguration wmrConfig,
		File languageConfDir, File tempDir, String jobName)
	{
		m_wmrConfig = wmrConfig;
		m_languageConfDir = languageConfDir;
		m_tempDir = tempDir;
		m_jobName = jobName;
		m_jobTempDir = null;
	}
	
	/**
	 * Prepares and writes a mapper script to the local filesystem, returning
	 * its path. Adds a shebang (#!) and wrapper code appropriate for the script
	 * language to the provided reducer source to produce the script.
	 *
	 * @param mapperSource The mapper code provided by the user
	 * @param language     The language of the mapper, one specified in the
	 *                     WebMapReduce language configuration
	 *
	 * @return The path to the script
	 */
	public File prepareMapperPackage(String mapperSource, String language)
		throws IOException, ValidationException, CompilationException
	{
		return prepareTransformPackage(TransformType.MAPPER, mapperSource,
			language);
	}
	
	/**
	 * Prepares and writes a reducer script to the local filesystem, returning
	 * its path. Adds a shebang (#!) and wrapper code appropriate for the script
	 * language to the provided reducer source to produce the script.
	 *
	 * @param reducerSource The reducer code provided by the user
	 * @param language      The language of the reducer, one specified in the
	 *                      WebMapReduce language configuration
	 *
	 * @return The path to the script
	 */
	public File prepareReducerPackage(String reducerSource,	String language)
		throws IOException, ValidationException, CompilationException
	{
		return prepareTransformPackage(TransformType.REDUCER, reducerSource,
			language);
	}
	
	public File getPackageDir() throws IOException
	{
		return getJobTempDir();
	}
	
	private File getJobTempDir() throws IOException
	{
		if (m_jobTempDir == null)
		{
			// Thanks to http://stackoverflow.com/a/1305966
			final File sysTempDir = new File(System.getProperty("java.io.tmpdir"));
			File newTempDir;
			final int maxAttempts = 9;
			int attemptCount = 0;
			do
			{
				attemptCount++;
				if (attemptCount > maxAttempts)
				{
					throw new IOException(
							"The highly improbable has occurred! " +
						       	"Failed to create a unique temporary " +
						        "directory after " + maxAttempts + 
							" attempts.");
				}
				String dirName = java.util.UUID.randomUUID().toString();
				newTempDir = new File(sysTempDir, dirName);
			} while (newTempDir.exists());

			if(newTempDir.mkdirs())
			{
				m_jobTempDir = newTempDir;
			}
			else
			{
				throw new IOException("Could not create local temporary directory.");
			}


		}
		
		return m_jobTempDir;
	}
	
	/**
	 * Internal implementation of #prepareMapperPackage() and
	 * #prepareReducerPackage() that works for either, determined by the
	 * transformType argument.
	 */
	protected File prepareTransformPackage(TransformType transformType,
		String transformSource, String language)
		throws IOException, ValidationException, CompilationException
	{
		// Get the given language's configuration, checking whether the language
		// exists in the configuration in the first place
		SubnodeConfiguration languageConf;
		try
		{
			languageConf = m_wmrConfig.configurationAt(language);
		}
		catch (IllegalArgumentException ex)
		{
			throw new ValidationException(
				"Specified source code language is not recognized.",
				language);
		}
		
		// Get the transform type in string form ("mapper" or "reducer") for use
		// in messages, etc.
		String transformTypeString = transformType.toString().toLowerCase();
		
		// Wrap source in library code as appropriate
		transformSource = wrap(languageConf, transformTypeString, transformSource);
		
		// Create temp directory to store transform package
		File jobTempDir = getJobTempDir();
		File jobTransformDir = new File(jobTempDir, transformTypeString);
		if (!jobTransformDir.mkdir())
		{
			throw new IOException("Could not create " + transformTypeString + " temporary directory:\n" +
				jobTransformDir.toString());
		}
		
		// Write processed transform source to disk
		String scriptExtension = languageConf.getString("extension", "");
		if (!scriptExtension.isEmpty())
			scriptExtension = "." + scriptExtension;
		File transformFile = writeTransformSource(transformTypeString,
			jobTransformDir, scriptExtension, transformSource);
		
		// Copy extra library files unless otherwise specified
		boolean copyLibraryFiles = languageConf.getBoolean("copyLibraryFiles", true);
		if (copyLibraryFiles)
		{
			copyLibraryFiles(languageConf, jobTransformDir);
		}
		
		// Compile if specified
		File srcTransformFile = transformFile;
		transformFile = compile(languageConf, transformTypeString, transformFile,
			jobTransformDir);
		if (!transformFile.equals(srcTransformFile))
			srcTransformFile.delete();
		
		// Return the path to the final transform file, relativized to the temp dir
		return relativizeFile(transformFile, getJobTempDir());
	}
	
	private String wrap(SubnodeConfiguration languageConf, String transformTypeString, String transformSource)
		throws IOException
	{
		// Get any wrapping code appropriate for the specified language,
		// including the shebang
		
		StrBuilder prefix = new StrBuilder();
		StrBuilder suffix = new StrBuilder();
		
		// Include the shebang???
		String interp = languageConf.getString("interpreter");
		if (interp != null && !interp.isEmpty())
			prefix.append("#!" + languageConf.getString("interpreter") + "\n");
		
		// Get the file extension appropriate for the language
		String scriptExtension = languageConf.getString("extension", "");
		if (!scriptExtension.isEmpty())
			scriptExtension = "." + scriptExtension;
		
		// Include the wrapper libraries, which potentially handle I/O, etc.
		File libDir = getLibraryDirectory(languageConf);
		if (libDir != null)
		{
			if (!libDir.isDirectory() || !libDir.canRead())
				throw new IOException(
				  "Library directory for " + transformTypeString + " did not exist " +
					"or was not readable: " + libDir.toString());
			
			Reader libReader = null;
			try
			{
				File libFile = new File(libDir, transformTypeString + "-prefix" + scriptExtension);
				if (libFile.isFile() && libFile.canRead())
				{
					libReader = new FileReader(libFile);
					IOUtils.copy(libReader, prefix.asWriter());
					IOUtils.closeQuietly(libReader);
				}
				
				libFile = new File(libDir, transformTypeString + "-suffix" + scriptExtension);
				if (libFile.isFile() && libFile.canRead())
				{
					libReader = new FileReader(libFile);
					IOUtils.copy(libReader, suffix.asWriter());
				}
			}
			catch (IOException ex)
			{
				throw new IOException(
					"Could not add the wrapper library to the " +
					transformTypeString + " code due to an exception.",
					ex);
			}
			finally
			{
				IOUtils.closeQuietly(libReader);
			}
		}
		
		return prefix.toString() + "\n" + transformSource + "\n" + suffix.toString();
	}
	
	private File writeTransformSource(String transformTypeString, File jobTransformDir,
		String scriptExtension, String transformSource) throws IOException
	{	
		// Write the script to a temporary file
		File scriptFile = new File(jobTransformDir, "job-" + transformTypeString + scriptExtension);
		if (!scriptFile.createNewFile())
		{
			throw new IOException("Could not create " + transformTypeString + " file.");
		}
		
		try
		{
			FileUtils.writeStringToFile(scriptFile, transformSource);
		}
		catch (IOException ex)
		{
			throw new IOException(
				"Could not write " + transformTypeString + " code to disk.",
				ex);
		}
		
		// Change file permissions on the new file to ensure it is accessible
		try
		{
			scriptFile.setReadable(true);
			scriptFile.setExecutable(true, false);
		}
		catch (SecurityException ex)
		{
			throw new IOException(
				"Could not assign permissions to " + transformTypeString + " file.",
				ex);
		}
		
		return scriptFile;
	}
	
	private File compile(SubnodeConfiguration languageConf,
		String transformTypeString,	File srcTransformFile, File jobTransformDir)
		throws CompilationException, IOException
	{
		// Determine correct compiler, returning if none specified
		String compiler = languageConf.getString("compiler-" + transformTypeString, "");
		if (compiler.isEmpty())
			compiler = languageConf.getString("compiler", "");
		if (compiler.isEmpty())
			return srcTransformFile;
		
		// Determine destination filename
		File compiledTransformFile = new File(jobTransformDir, "compiled-job-" + transformTypeString);
		
		// Create map to replace ${wmr:...} variables.
		// NOTE: Commons Configuration's built-in interpolator does not work here
		//  for some reason.
		File jobTempDir = getJobTempDir();
		Hashtable<String, String> variableMap = new Hashtable<String, String>();
		File libDir = getLibraryDirectory(languageConf);
		variableMap.put("wmr:lib.dir", (libDir != null) ? libDir.toString() : "");
		variableMap.put("wmr:src.dir",
			relativizeFile(srcTransformFile.getParentFile(), jobTempDir).toString());
		variableMap.put("wmr:src.file",
			relativizeFile(srcTransformFile, jobTempDir).toString());
		variableMap.put("wmr:dest.dir",
			relativizeFile(jobTransformDir, jobTempDir).toString());
		variableMap.put("wmr:dest.file",
			relativizeFile(compiledTransformFile, jobTempDir).toString());

		// Replace variables in compiler string
		compiler = StrSubstitutor.replace(compiler, variableMap);
		
		
		// Run the compiler
		
		CommandLine compilerCommand = CommandLine.parse(compiler);
		DefaultExecutor exec = new DefaultExecutor();
		ExecuteWatchdog dog = new ExecuteWatchdog(60000); // 1 minute
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		PumpStreamHandler pump = new PumpStreamHandler(output);
		exec.setWorkingDirectory(jobTempDir);
		exec.setWatchdog(dog);
		exec.setStreamHandler(pump);
		exec.setExitValues(null);  // Can't get the exit code if it throws exception
		
		int exitStatus = -1;
		try
		{
			exitStatus = exec.execute(compilerCommand);
		}
		catch (IOException ex)
		{
			// NOTE: Exit status is still -1 in this case, since exception was thrown
			throw new CompilationException("Compiling failed for " + transformTypeString,
			  exitStatus, new String(output.toByteArray()));
		}
		
		// Check for successful exit
		
		if (exitStatus != 0)
			throw new CompilationException("Compiling failed for " + transformTypeString,
				exitStatus, new String(output.toByteArray()));
		
		// Check that output exists and is readable, and make it executable
		
		if (!compiledTransformFile.isFile())
			throw new CompilationException("Compiler did not output a " +
				transformTypeString + " executable (or it was not a regular file).",
			  exitStatus, new String(output.toByteArray()));
		
		if (!compiledTransformFile.canRead())
			throw new IOException(StringUtils.capitalize(transformTypeString) +
				" executable output from compiler was not readable: " +
				compiledTransformFile.toString());
		
		if (!compiledTransformFile.canExecute())
			compiledTransformFile.setExecutable(true, false);
		
		
		return compiledTransformFile;
	}
	
	private void copyLibraryFiles(SubnodeConfiguration languageConf,
		File jobTransformDir)
		throws IOException
	{
		// Get the library directory, returning if not specified
		File libDir = getLibraryDirectory(languageConf);
		if (libDir == null)
			return;
		
		// Get the file extension appropriate for the language
		String scriptExtension = languageConf.getString("extension", "");
		if (!scriptExtension.isEmpty())
			scriptExtension = "." + scriptExtension;
		
		// Create a file filter to exclude prefix/suffix files
		String pattern = "^(mapper|reducer)-(prefix|suffix)" +
			Pattern.quote(scriptExtension) + "$";
		FileFilter libFilter = new NotFileFilter(new RegexFileFilter(pattern));
		
		// Copy filtered contents of library directory
		FileUtils.copyDirectory(libDir, jobTransformDir, libFilter);
	}
	
	private File relativizeFile(File file, File relativeToFile)
	{
		URI fileURI = file.toURI();
		URI rtURI = relativeToFile.getAbsoluteFile().toURI();
		return new File(rtURI.relativize(fileURI).getPath());
	}
	
	private File getLibraryDirectory(SubnodeConfiguration languageConf)
	{
		String libDirString = languageConf.getString("library", "");
		if (libDirString.isEmpty())
			return null;
		File libDir = new File(libDirString);
		if (!libDir.isAbsolute())
			libDir = new File(m_languageConfDir, libDirString);
		return libDir;
	}
}

/**
 * Specifies whether a transform is a mapper or reducer.
 */
enum TransformType
{
	MAPPER,
	REDUCER
}
