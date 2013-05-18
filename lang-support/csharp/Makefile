# Copyright 2012 WebMapReduce Developers
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

# Makefile for compiling C# mappers and reducers in WebMapReduce
#
# When compiling user-writing mapper and reducer files, the following variables need to be defined on the command line
#
#   ASSEM_FILE:Either "job-mapper.exe" or "job-reducer.exe"
#   DEST_DIR:  The value of ${wmr:dest.dir}
#   DEST_FILE: The value of ${wmr:dest.file}
#   SRC_DIR:   The value of ${wmr:src.dir}
#   SRC_FILE:  The value of ${wmr:src.file}
#
#
#
#
#
#

LIB = $(DEST_DIR)/Wmr.cs $(DEST_DIR)/WmrCommonImpl.cs 

default: all

all: $(ASSEM_FILE)
	echo "mono $(DEST_DIR)/$(ASSEM_FILE)" > $(DEST_FILE)
	chmod a+x $(DEST_FILE) 

$(ASSEM_FILE):
	mcs -r:Mono.Posix -o:$(DEST_DIR)/$@ $(SRC_FILE) $(LIB)
