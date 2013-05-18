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

import java.io.File;

/**
 * Holds the results of test jobs.
 */
public class TestJobResult
{
	TransformResult _mapResult;
	TransformResult _reduceResult;
	
	public TestJobResult() { }
	
	public void setMapResult(TransformResult mapResult) {
		_mapResult = mapResult;
	}
	
	public TransformResult getMapResult() {
		return _mapResult;
	}
	
	public void setReduceResult(TransformResult reduceResult) {
		_reduceResult = reduceResult;
	}
	
	public TransformResult getReduceResult() {
		return _reduceResult;
	}
	
	/**
	 * Holds the results of individual transforms (mapper and reducer).
	 */
	public static class TransformResult
	{
		int _exitCode;
		File _outputFile;
		File _errorFile;
		
		public TransformResult() { }
		
		public void setExitCode(int exitCode) {
			_exitCode = exitCode;
		}
		
		public int getExitCode() {
			return _exitCode;
		}
		
		public void setOutputFile(File outputFile) {
			_outputFile = outputFile;
		}
		
		public File getOutputFile() {
			return _outputFile;
		}
		
		public void setErrorFile(File errorFile) {
			_errorFile = errorFile;
		}
		
		public File getErrorFile() {
			return _errorFile;
		}
	}
}
