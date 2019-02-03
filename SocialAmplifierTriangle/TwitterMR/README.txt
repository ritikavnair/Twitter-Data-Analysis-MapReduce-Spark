Count Twitter Followers - In Hadoop MapReduce
CS6240 Fall 2018 HW1
=================================================

Code author
-----------
Ritika Nair

Installation
------------
These components are installed:
- JDK 1.8
- Hadoop 2.9.1
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/home/joe/tools/hadoop/hadoop-2.9.1
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

All of the build & execution commands are organized in the Makefile.

Execution
---------

1) Clone this repository to your local machine by using the following command in git bash:
	git clone https://github.ccs.neu.edu/cs6240f18/ritikanair.git
	cd HW2
	
2) Open an instance of Command Prompt/Terminal here.

3) Navigate inside the "TwitterMR" folder:
	cd TwitterMR
	
4) In the 'input' folder, place the 'edges.csv' file from the Twitter dataset. I have not provided this
   file as part of this zip since it is a huge file.
   
5) Update the following variables in the Makefile to customize according to your setup:
    hadoop.root, 
    hdfs.user.name, 
    aws.bucket.name,
    job.name : Set this value as the name of the class you wish to run.
    eg: to run the cardinality program, set this value as follows:
    job.name=TwitterAnalysis.Path2RSJoinCardinality
    
6) To run in standalone mode use the commands:
	make switch-standalone		
	make local
	
7) To run in Pseudo-Distributed Hadoop: 
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running 

8) To run on AWS EMR Hadoop: 
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	make download-output-aws	-- after successful execution & termination
	
An output folder is created with the output files inside it in the same /TwitterMR/ directory.
	
REFERENCES:
===========
1. MR-Demo and Spark-Demo code given by professor.
