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

#include "wmr_common.h"

#include <cstdio>
#include <cstring>
#include <iostream>
#include <sstream>

namespace wmr
{
	namespace internal
	{
		bool wmr_common::s_eof = false;
		char wmr_common::s_delim = '\t';
		
		long wmr_common::getline(std::string & str)
		{
			using std::cin;
			s_eof = (cin.peek() == EOF);
			
			if (s_eof)
			{
				return -1;
			}
			else
			{
				std::getline(cin, str);
				return str.length();
			}
		}
		
		char wmr_common::delimiter(void)
		{
			return s_delim;
		}
	}
	
	std::vector<std::string> utility::splitString(const std::string & str,
		char delim)
	{
		std::vector<std::string> tokens;
		std::stringstream ss(str);
		std::string token;
		
		while (std::getline(ss, token, delim))
		{
			tokens.push_back(token);
		}
		
		return tokens;
	}
}
