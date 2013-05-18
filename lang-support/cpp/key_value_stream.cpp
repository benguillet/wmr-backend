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

#include "key_value_stream.h"

namespace wmr
{
	namespace internal
	{
		std::string key_value_stream::m_key("");
		std::string key_value_stream::m_value("");
		bool key_value_stream::m_eof = false;
		
		// feed the key_value_stream by getting and parsing a line of
		// key-value input from standard input
		bool key_value_stream::getline(void)
		{
			using std::string;
			using wmr::internal::wmr_common;
			
			// don't do anything unnecessary
			if (m_eof) return false;
			
			string line("");
			m_key = line;
			m_value = line;
			
			// get input, and make sure that we haven't hit eof
			long length = wmr_common::getline(line);
			
			// getline returns -1 on eof
			if (length < 0)
			{
				m_eof = true;
				return false;
			}
			
			// parse the key and value of the line
			size_t delimIndex = string::npos;
			delimIndex = line.find_first_of(wmr_common::delimiter());
			
			if (delimIndex == string::npos)
			{
				m_key.assign("");
				m_value.assign(line);
			}
			else
			{
				string extractKey(line, 0, delimIndex);
				string extractValue(line, delimIndex + 1, string::npos);
				
				m_key.assign(extractKey);
				m_value.assign(extractValue);
			}
			
			return true;
		}
		
		const std::string & key_value_stream::key(void)
		{
			return m_key;
		}
		
		const std::string & key_value_stream::value(void)
		{
			return m_value;
		}
		
		bool key_value_stream::eof(void)
		{
			return m_eof;
		}
	}
}
