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

  public class WmrReducerImpl {

    public static string nextKey = null; 
    public static string nextValue = null;  

    public static bool endInput() {
      return nextKey == null;
    }
    
    public static void advance() {
      string line = null;
      line = WmrCommonImpl.getLine();
      if (line == null) {
	nextKey = nextValue = null;
	return;
      }
    
      int delimIndex = line.IndexOf(WmrCommonImpl.delim);
      if (delimIndex == -1) {
	nextKey = line;
	nextValue = "";
      } else {
	nextKey = line.Remove(delimIndex);
	nextValue = line.Remove(0,delimIndex+1);
      }
    }


  public static void Main() {
    Reducer red = new Reducer();
    advance();
    while (!endInput()) {
      red.reduce(nextKey, new WmrIterator(nextKey));
    }
  }
  }
    
}

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
