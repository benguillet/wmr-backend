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

// C# for St. Olaf WMR Project
// By Boyang Wei
// 2011 Summer

namespace WMR {

  public class WmrIterator : System.Collections.IEnumerable {
	
    string myKey;

    public WmrIterator(string key) {
      myKey = key;
    }

    public bool hasNext() {
      return !WmrReducerImpl.endInput() && (WmrReducerImpl.nextKey.Equals(myKey));
    }

    public System.Collections.IEnumerator GetEnumerator() {
      while (hasNext()) {
	string value = WmrReducerImpl.nextValue;
	WmrReducerImpl.advance();
	yield return value; 
      } 
    }

  }

}
