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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import edu.stolaf.cs.wmrserver.db.SubmissionDatabase;
import edu.stolaf.cs.wmrserver.thrift.JobService;

@SuppressWarnings("deprecation")
public class ThriftServer extends Configured implements Tool
{
	public static final String CONF_DEFAULT_RESOURCE = "wmr-default.xml";
	public static final String CONF_SITE_RESOURCE = "wmr-site.xml";
	
	static
	{
		Configuration.addDefaultResource(CONF_DEFAULT_RESOURCE);
		Configuration.addDefaultResource(CONF_SITE_RESOURCE);
	}
	
	public static void main(String[] args) throws Exception
	{
		ThriftServer server = new ThriftServer();
		ToolRunner.run(server, args);
	}
	
	public int run(String[] args) throws Exception
	{
		Configuration conf = getConf();
		int port = conf.getInt("wmr.server.bind.port", 50100);
		
		SubmissionDatabase.connect(conf);
		
		JobServiceHandler service = new JobServiceHandler(new Configuration());
		JobService.Processor processor = new JobService.Processor(service);
		TServerTransport transport = new TServerSocket(port);
		TServer server = new TSimpleServer(new Args(transport).processor(processor));
		
		server.serve();
		
		return 0;
	}
}
