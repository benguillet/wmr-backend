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

import org.apache.hadoop.fs.Path;

import edu.stolaf.cs.wmrserver.db.Submission;
import edu.stolaf.cs.wmrserver.thrift.*;


public interface JobEngine
{
	void submit(JobRequest request, long submissionID, File mapperFile,
			File reducerFile, File packageDir, Path inputPath)
		throws ValidationException, NotFoundException,
		       CompilationException, InternalException,
		       ForbiddenTestJobException;
	
	JobStatus getStatus(Submission submission)
		throws NotFoundException, InternalException;
	
	void kill(Submission submission)
		throws NotFoundException, IllegalJobStateException, InternalException;
}
