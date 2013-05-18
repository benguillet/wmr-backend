/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Modified by the WebMapReduce developers.
 */

package edu.stolaf.cs.wmrserver.streaming;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;

/** All the client-side work happens here.
 * (Jar packaging, MapRed job submission and monitoring)
 */
public class StreamJob {

  protected static final Log LOG = LogFactory.getLog(StreamJob.class.getName());
    
  private StreamJob() { }
	
	public static void setStreamMapper(JobConf conf, String mapCommand)
	{
		conf.setMapperClass(PipeMapper.class);
		conf.setMapRunnerClass(PipeMapRunner.class);
		try {
			conf.set("stream.map.streamprocessor", 
			         URLEncoder.encode(mapCommand, "UTF-8"));
		}
		catch (UnsupportedEncodingException ex) {
			// This is VERY likely to happen. Especially since the ENTIRE FREAKING
			// STRING IMPLEMENTATION is based on UTF-8. Thanks, Java.
			throw new RuntimeException("The sky is falling! Java doesn't support UTF-8.");
		}
	}
	
	public static void setStreamReducer(JobConf conf, String reduceCommand)
	{
		conf.setReducerClass(PipeReducer.class);
		try {
			conf.set("stream.reduce.streamprocessor",
			         URLEncoder.encode(reduceCommand, "UTF-8"));
		}
		catch (java.io.UnsupportedEncodingException ex) {
			throw new RuntimeException("The sky is falling! Java doesn't support UTF-8.");
		}
	}

	public static String createJobJar(JobConf conf, List extraFiles)
		throws IOException
	{
		return createJobJar(conf, extraFiles, null);
	}
	
	public static String createJobJar(JobConf conf, List extraFiles,
		File tmpDir) throws IOException
	{
		ArrayList unjarFiles = new ArrayList();
		ArrayList packageFiles = new ArrayList(extraFiles);
		
    // Runtime code: ship same version of code as self (job submitter code)
    // usually found in: build/contrib or build/hadoop-<version>-dev-streaming.jar
		
    // First try an explicit spec: it's too hard to find our own location in this case:
    // $HADOOP_HOME/bin/hadoop jar /not/first/on/classpath/custom-hadoop-streaming.jar
    // where findInClasspath() would find the version of hadoop-streaming.jar in $HADOOP_HOME
    String runtimeClasses = conf.get("stream.shipped.hadoopstreaming"); // jar or class dir
		
		if (runtimeClasses == null) {
      runtimeClasses = StreamUtil.findInClasspath(StreamJob.class);
    }
    if (runtimeClasses == null) {
      throw new IOException("runtime classes not found: " + StreamJob.class.getPackage());
    }
		if (StreamUtil.isLocalJobTracker(conf)) {
      // don't package class files (they might get unpackaged in "." and then
      //  hide the intended CLASSPATH entry)
      // we still package everything else (so that scripts and executable are found in
      //  Task workdir like distributed Hadoop)
    } else {
      if (new File(runtimeClasses).isDirectory()) {
        packageFiles.add(runtimeClasses);
      } else {
        unjarFiles.add(runtimeClasses);
      }
    }
    if (packageFiles.size() + unjarFiles.size() == 0) {
      return null;
    }
		if (tmpDir == null)
		{
			String tmp = conf.get("stream.tmpdir", ""); //, "/tmp/${user.name}/"
			tmpDir = new File(tmp);
		}
    // tmpDir=null means OS default tmp dir
		File jobJar = File.createTempFile("streamjob", ".jar", tmpDir);
		System.out.println("packageJobJar: " + packageFiles + " " + unjarFiles + " " + jobJar
		                   + " tmpDir=" + tmpDir);
		jobJar.deleteOnExit();
		JarBuilder builder = new JarBuilder();
		String jobJarName = jobJar.getAbsolutePath();
		builder.merge(packageFiles, unjarFiles, jobJarName);
		return jobJarName;
	}
	
	public static void addTaskEnvironment(JobConf conf, Map<String, String> vars)
	{
		// Encode the variables as "key1=val1 key2=val2". This is a very fragile
		// encoding, but we work with what we are given...
		String varsString = "";
		for (Map.Entry<String, String> var : vars.entrySet())
			varsString += var.getKey() + "=" + var.getValue() + " ";
		
		addTaskEnvironment(conf, varsString.trim());
	}
	
	protected static void addTaskEnvironment(JobConf conf, String vars)
	{
		String previousVars = getTaskEnvironment(conf);
		if (previousVars != null && !previousVars.trim().isEmpty())
			vars = previousVars + " " + vars;
		
		setTaskEnvironment(conf, vars);
	}
	
	protected static void setTaskEnvironment(JobConf conf, String vars)
	{
    conf.set("stream.addenvironment", vars);
	}
	
	protected static String getTaskEnvironment(JobConf conf)
	{
		return conf.get("stream.addenvironment");
	}
}
