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

#include "datastream.h"

namespace wmr
{
	datastream::datastream(void)
	{
		m_endOfKey = true;
	}
	
	datastream::datastream(std::string key)
	{
		m_key = key;
		m_endOfKey = false;
		m_value = internal::key_value_stream::value();
	}
	
	datastream::datastream(const datastream & other)
	{
		m_key = other.key();
		m_value = other.value();
		m_endOfKey = other.eof();
	}
	
	datastream::~datastream(void)
	{
	}
	
	datastream & datastream::operator>>(std::stringstream & output)
	{
		return this->get(output);
	}
	
	datastream & datastream::get(std::stringstream & output)
	{
		// yield the current value
		output << m_value;
		
		// advance to the next value in the stream
		this->advance();
		
		return *this;
	}
	
	void datastream::advance(void)
	{
		if (m_endOfKey) return;
	
		// get the next key in the stream
		internal::key_value_stream::getline();
		std::string key(internal::key_value_stream::key());
		
		// check if the stream is still valid for this key
		if (m_key != key)
		{
			m_endOfKey = true;
			m_key = key;
		}
		
		// assign the next value
		m_value = internal::key_value_stream::value();
	}
}
