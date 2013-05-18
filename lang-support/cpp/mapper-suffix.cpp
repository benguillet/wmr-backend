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

int main(int argc, char ** argv)
{
	mapper m;
	std::string empty(""), line;
	long length = -1;
	
	length = wmr::internal::wmr_common::getline(line);
	while (length >= 0)
	{
		size_t delimIndex = std::string::npos;
		delimIndex = line.find_first_of(wmr::internal::wmr_common::delimiter());
		
		if (delimIndex == std::string::npos)
		{
			m.map(line, empty);
		}
		else
		{
			std::string k(line, 0, delimIndex);
			std::string v(line, delimIndex + 1, std::string::npos);
			m.map(k, v);
		}
		
		length = wmr::internal::wmr_common::getline(line);
	}
	
	return 0;
}

/*
	Mapper Class Template
	
	class mapper
	{
	public:
		mapper(void);
		~mapper(void);
		void map(std::string key, std::string value);
	};
*/
