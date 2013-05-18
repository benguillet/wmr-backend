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

# Makefile for compiling Java mappers and reducers in WebMapReduce
#
# When compiling user-writen mapper and reducer files, the following variables
# need to be defined on the command line:
# 
#   TRANSFORM: "Mapper" or "Reducer"
#   DEST_DIR:  The value of ${wmr:dest.dir}
#   DEST_FILE: The value of ${wmr:dest.file}
#   SRC_DIR:   The value of ${wmr:src.dir}
#   SRC_FILE:  The value of ${wmr:src.file}
#
# This makefile may contain features specific to GNU make

default: all


# Defaults if not set on command line
SRC_DIR = .
DEST_DIR = .

# Values that depend the target transform
MAIN_CLASSNAME=Wmr$(TRANSFORM)Impl
MAIN_CLASS=$(DEST_DIR)/Wmr$(TRANSFORM)Impl.class
MAIN_SRC=$(DEST_DIR)/Wmr$(TRANSFORM)Impl.java
TRANSFORM_SRC=$(DEST_DIR)/$(TRANSFORM).java
TRANSFORM_CLASS=$(DEST_DIR)/$(TRANSFORM).class

# Library files
LIB_CLASSES = $(DEST_DIR)/Wmr.class $(DEST_DIR)/WmrCommonImpl.class $(DEST_DIR)/Wmr$(TRANSFORM)Impl.class


# Java auto-compilation
.SUFFIXES: .class .java
.java.class:
	javac -sourcepath $(DEST_DIR):$(SRC_DIR) -d $(DEST_DIR) $*.java 
all: $(TRANSFORM_CLASS) $(MAIN_CLASS) library $(DEST_FILE)

library: $(LIB_CLASSES) $(TRANSFORM_SRC) $(MAIN_CLASS)

# Rename source file to approriate class to meet Java requirements
$(TRANSFORM_SRC): $(SRC_FILE)
	mv $(SRC_FILE) $(TRANSFORM_SRC)

# Launcher script
$(DEST_FILE):
	sed -e "s|{{CLASSPATH}}|$(DEST_DIR)/|g" \
	    -e "s|{{CLASSNAME}}|$(MAIN_CLASSNAME)|g" \
	    $(DEST_DIR)/launcher.sh > $@
	chmod a+x $(DEST_FILE)
