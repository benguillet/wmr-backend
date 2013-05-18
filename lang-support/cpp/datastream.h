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

#ifndef __WMR_DATASTREAM_H_
#define __WMR_DATASTREAM_H_

#include <sstream>
#include "key_value_stream.h"

namespace wmr
{
	class datastream
	{
	private:
		std::string m_key;
		std::string m_value;
		bool m_endOfKey;
	
	public:
		datastream(void);
		datastream(std::string);
		datastream(const datastream &);
		~datastream(void);
		
		bool eof(void) const { return m_endOfKey; }
		const std::string & associated_key(void) const { return m_key; }
		
		template <typename T>
		datastream & operator>>(T &);
		template <typename T>
		datastream & get(T &);
		
		datastream & operator>>(std::stringstream &);
		datastream & get(std::stringstream &);
		
	private:
		std::string key(void) const { return m_key; }
		std::string value(void) const { return m_value; }
		void advance(void);
	};
	
	template <typename T>
	datastream & datastream::operator>> (T & out)
	{
		return this->get<T>(out);
	}
	
	template <typename T>
	datastream & datastream::get(T & out)
	{
		std::stringstream ss;
		
		// Get the current value, and convert it to T
		ss << m_value;
		ss >> out;
		
		// Advance the stream and return
		this->advance();
		return *this;
	}
}

#endif
