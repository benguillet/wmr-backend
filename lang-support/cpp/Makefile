# Copyright 2010 WebMapReduce Developers
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

default: build

MAPRED_SRC=DefaultSrc.cpp
MAPRED_DEST_DIR=DefaultDir
MAPRED_DEST_FILE=DefaultApp

COMMON_OBJS=$(MAPRED_DEST_DIR)/wmr_common.o $(MAPRED_DEST_DIR)/key_value_stream.o $(MAPRED_DEST_DIR)/datastream.o
TEST_OBJS=build/wmr_common.o build/key_value_stream.o build/datastream.o

library:
	g++ -Wall -c $(MAPRED_DEST_DIR)/wmr_common.cpp -o $(MAPRED_DEST_DIR)/wmr_common.o
	g++ -Wall -c $(MAPRED_DEST_DIR)/key_value_stream.cpp -o $(MAPRED_DEST_DIR)/key_value_stream.o
	g++ -Wall -c $(MAPRED_DEST_DIR)/datastream.cpp -o $(MAPRED_DEST_DIR)/datastream.o

build: library
	g++ -o $(MAPRED_DEST_FILE) $(MAPRED_SRC) $(COMMON_OBJS)

testbuild:
	g++ -Wall -c wmr_common.cpp -o build/wmr_common.o
	g++ -Wall -c key_value_stream.cpp -o build/key_value_stream.o
	g++ -Wall -c datastream.cpp -o build/datastream.o

testclean:
	rm -f build/*.o
	rm -f example/*.o

testmap: testbuild
	g++ -Wall -I/home/garrity/cppwrap -c example/mapper_test.cpp -o example/mapper_test.o
	g++ -o bin/mapper_test example/mapper_test.o $(TEST_OBJS)

testreduce: testbuild
	g++ -Wall -I/home/garrity/cppwrap -c example/reducer_test.cpp -o example/reducer_test.o
	g++ -o bin/reducer_test example/reducer_test.o $(TEST_OBJS)


