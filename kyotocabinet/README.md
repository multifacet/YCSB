<!--
Copyright (c) 2012 - 2018 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

Kyotocabinet
Using kyotocabinet-java wrapper.

### Download KyotoCabinet, extract the tar, and build it.

https://fallabs.com/kyotocabinet/

You will need to download and build the core libraries and the java API.

```
tar xvf kyotocabinet-1.2.77.tar.xz
cd kyotocabinet-1.2.77
./configure --prefix=`pwd`
make && make install
cd ..
tar xvf kyotocabinet-java-1.24.tar.xz
cd ../kyotocabinet-java-1.24
JAVA_HOME=/usr/lib/jvm/java-14-openjdk-amd64/ ./configure --with-kc=../kyotocabinet-1.2.77/
```

You need to have the following installed
- zlib: `libz-dev`
- java: `openjdk-14-jdk`

### Set Up YCSB

Clone the YCSB git repository and compile:

    git clone https://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package

Or,

    mvn -pl site.ycsb:kyotocabinet-binding -am clean package

### Run YCSB

Now you are ready to run! First, load the data:

    ./bin/ycsb load kyotocabinet -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run kyotocabinet -s -P workloads/workloada

## Kyotocabinet Configuration Parameters

* ```kc.dir``` - (required) A path to a folder to hold the Kyotocabinet data files.
    * EX. ```/tmp/kc.kch```
    * https://fallabs.com/kyotocabinet/javadoc/kyotocabinet/DB.html#open(java.lang.String,%20int)
    for open(..) path parameter

### Load data and run tests

Load the data:

    ./bin/ycsb load kyotocabinet -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run kyotocabinet -s -P workloads/workloada > outputRun.txt
