/* Copyright 2011 Boyang Wei, WebMapReduce developer

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */

namespace WMR {

  public class WmrMapperImpl {
	public static void Main() {
		Mapper map = new Mapper();
		string line;
		
		while ((line = WmrCommonImpl.getLine()) != null) {
			int delimIndex = line.IndexOf(WmrCommonImpl.delim);
			if (delimIndex == -1)
				map.map(line, "");
			else {
				map.map(line.Remove(delimIndex), line.Remove(0,delimIndex+1));
			}
		}
	}
  }

}
