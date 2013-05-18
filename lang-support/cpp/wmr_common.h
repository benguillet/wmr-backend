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

#ifndef __WMR_INTERNAL_WMR_COMMON_H_
#define __WMR_INTERNAL_WMR_COMMON_H_

#include <string>
#include <vector>
#include <sstream>
#include <iostream>

namespace wmr
{
	namespace internal
	{
		/*
			The wmr_common class is an internal helper class which wraps around
			the standard getline function, and keeps track of the delimiter.
			This delimiter is usually tab, but the can be adjusted to change how
			keys and values are split. It is meant to be used statically.
		*/
		class wmr_common
		{
		private:
			static bool s_eof;
			static char s_delim;
		
		public:
			/*
				Gets a string from standard input using getline, while checking
				for the end of input. This method returns the length of the
				acquired string, or -1 if the end of input is reached.
			*/
			static long getline(std::string &);
			static char delimiter(void);
		};
	}
	
	class utility
	{
	public:
		// generic conversion methods (stringstream)
		template <typename T>
		static std::string toString(const T &);
		template <typename T>
		static T fromString(const std::string &);
		
		// Methods like this make me want to die...
		static int stringToInt(const std::string &);
		static long stringToLong(const std::string &);
		static float stringToFloat(const std::string &);
		static double stringToDouble(const std::string &);
		static bool stringToBool(const std::string &);
		
		// split a string on a given delimiter
		static std::vector<std::string> split(const std::string &, char);
		
		// split a string on multiple delimiters
		static std::vector<std::string> split_multi(const std::string &, 
			const std::string &);
		
		static std::vector<std::string> splitString(const std::string &, char);
	};
	
	template <typename T>
	std::string utility::toString(const T & data)
	{
		std::stringstream ss;
		ss << data;
		return ss.str();
	}
	
	template <typename T>
	T utility::fromString(const std::string & str)
	{
		std::stringstream ss;
		ss << str;
		T data;
		ss >> data;
		return data;
	}
	
	inline int utility::stringToInt(const std::string & str)
	{
		return fromString<int>(str);
	}
	
	inline long utility::stringToLong(const std::string & str)
	{
		return fromString<long>(str);
	}
	
	inline float utility::stringToFloat(const std::string & str)
	{
		return fromString<float>(str);
	}
	
	inline double utility::stringToDouble(const std::string & str)
	{
		return fromString<double>(str);
	}
	
	inline bool utility::stringToBool(const std::string & str)
	{
		return fromString<bool>(str);
	}
	
	inline std::vector<std::string> utility::split(const std::string & str,
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
	
	inline std::vector<std::string> utility::split_multi(const std::string & str,
		const std::string & delim)
	{
		// Output vector
		std::vector<std::string> out;
	
		// initial checks:
		if (str.empty() || delim.empty()) return out;
	
		// position tracking variables
		size_t begin = 0;
		size_t end = 0;
	
		while (end < str.size() && (begin + 1) < str.size())
		{
			// Find the next delimiting character
			end = str.find_first_of(delim, begin);
			
			// If the delimiter was not found, the rest of the string is the
			// next token
			if (end == std::string::npos)
			{
				out.push_back(str.substr(begin, end));
				break;
			}
			
			// Otherwise, if two tokens are not side-by-side, we can output
			// one or more non-delimiting characters
			if (end - begin > 0)
			{
				out.push_back(str.substr(begin, (end - begin)));
			}
			
			// Advance to the next character
			begin = end + 1;
		}
	
		return out;
	}
	
	// emit two values separated by a delimiter
	template <typename T, typename K>
	void emit(const T & key, const K & value)
	{
		std::cout << key << internal::wmr_common::delimiter() << value
			<< std::endl;
	}
}

#endif
