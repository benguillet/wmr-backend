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

import java.io.*;
import java.net.*;
import java.util.Arrays;

import org.apache.hadoop.mapred.JobConf;

/** Utilities not available elsewhere in Hadoop.
 *  
 */
public class StreamUtil {
	
	public static URL getClassResource(Class<?> clazz) {
		String simpleName = clazz.getSimpleName();
		return clazz.getResource(simpleName + ".class");
	}
	
	public static String classNameToPath(String className) {
    String relPath = className;
    relPath = relPath.replace('.', '/');
    relPath += ".class";
		return relPath;
	}
	
	public static URL getClassResource(String className, ClassLoader loader) {
    return loader.getResource(classNameToPath(className));
	}
	
	public static String getClassBasePath(URL classUrl, Class<?> clazz) {
    return getClassBasePath(classUrl, clazz.getName());
	}
	
	public static String getClassBasePath(URL classUrl, String className) {
		String codePath;
    if (classUrl != null) {
      boolean inJar = classUrl.getProtocol().equals("jar");
      codePath = classUrl.toString();
      if (codePath.startsWith("jar:")) {
        codePath = codePath.substring("jar:".length());
      }
      if (codePath.startsWith("file:")) { // can have both
        codePath = codePath.substring("file:".length());
      }
      if (inJar) {
        // A jar spec: remove class suffix in /path/my.jar!/package/Class
        int bang = codePath.lastIndexOf('!');
        codePath = codePath.substring(0, bang);
      } else {
        // A class spec: remove the /my/package/Class.class portion
        int pos = codePath.lastIndexOf(classNameToPath(className));
        if (pos == -1) {
          throw new IllegalArgumentException("invalid codePath: className=" + className
                                             + " codePath=" + codePath);
        }
        codePath = codePath.substring(0, pos);
      }
    } else {
      codePath = null;
    }
    return codePath;
	}

  public static String findInClasspath(String className) {
    return findInClasspath(className, StreamUtil.class.getClassLoader());
  }

  /** @return a jar file path or a base directory or null if not found.
   */
  public static String findInClasspath(String className, ClassLoader loader){
    URL classUrl = getClassResource(className, loader);
    return getClassBasePath(classUrl, className);
  }
	
	public static String findInClasspath(Class<?> clazz) {
		URL classUrl = getClassResource(clazz);
		return getClassBasePath(classUrl, clazz);
	}

  static class StreamConsumer extends Thread {

    StreamConsumer(InputStream in, OutputStream out) {
      this.bin = new LineNumberReader(new BufferedReader(new InputStreamReader(in)));
      if (out != null) {
        this.bout = new DataOutputStream(out);
      }
    }

    public void run() {
      try {
        String line;
        while ((line = bin.readLine()) != null) {
          if (bout != null) {
            bout.writeUTF(line); //writeChars
            bout.writeChar('\n');
          }
        }
        bout.flush();
      } catch (IOException io) {
      }
    }

    LineNumberReader bin;
    DataOutputStream bout;
  }

  static void exec(String arg, PrintStream log) {
    exec(new String[] { arg }, log);
  }

  static void exec(String[] args, PrintStream log) {
    try {
      log.println("Exec: start: " + Arrays.asList(args));
      Process proc = Runtime.getRuntime().exec(args);
      new StreamConsumer(proc.getErrorStream(), log).start();
      new StreamConsumer(proc.getInputStream(), log).start();
      int status = proc.waitFor();
      //if status != 0
      log.println("Exec: status=" + status + ": " + Arrays.asList(args));
    } catch (InterruptedException in) {
      in.printStackTrace();
    } catch (IOException io) {
      io.printStackTrace();
    }
  }

  // JobConf helpers

  static class TaskId {

    boolean mapTask;
    String jobid;
    int taskid;
    int execid;
  }

  public static boolean isLocalJobTracker(JobConf job) {
    return job.get("mapred.job.tracker", "local").equals("local");
  }

  public static TaskId getTaskInfo(JobConf job) {
    TaskId res = new TaskId();

    String id = job.get("mapred.task.id");
    if (isLocalJobTracker(job)) {
      // it uses difft naming 
      res.mapTask = job.getBoolean("mapred.task.is.map", true);
      res.jobid = "0";
      res.taskid = 0;
      res.execid = 0;
    } else {
      String[] e = id.split("_");
      res.mapTask = e[3].equals("m");
      res.jobid = e[1] + "_" + e[2];
      res.taskid = Integer.parseInt(e[4]);
      res.execid = Integer.parseInt(e[5]);
    }
    return res;
  }
}
