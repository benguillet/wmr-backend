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

#ifndef __WMR_INTERNAL_KEY_VALUE_STREAM_H_
#define __WMR_INTERNAL_KEY_VALUE_STREAM_H_

#include "wmr_common.h"

namespace wmr
{
	namespace internal
	{
		/*
			The key_value_stream class is responsible for managing standard
			input, and turning it into key-value pairs which can be passed on to
			other objects, specifically a mapper or datastream. It is meant to
			be used statically, and is not concerned with what the key is. The 
			eof state is set when standard input is empty or yields eof.
		*/
		class key_value_stream
		{
		private:
			static std::string m_key;
			static std::string m_value;
			static bool m_eof;
		
		public:
			/*
				Gets the next line of standard input, transforming it into a
				key-value pair which is stored in the static variables m_key and
				m_value. Returns false if key_value_stream is in (or enters) an
				eof state; returns true otherwise.
			*/
			static bool getline(void);
			static const std::string & key(void);
			static const std::string & value(void);
			static bool eof(void);
		};
	}
}

#endif
