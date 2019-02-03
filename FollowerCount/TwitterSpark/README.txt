Count Twitter Followers - Using Spark
CS6240 Fall 2018 HW1
=================================================

Code author
-----------
Ritika Nair

Installation
------------
These components are installed:
- JDK 1.8
- Scala 2.11.12
- Hadoop 2.9.1
- Spark 2.3.1 (without bundled Hadoop)
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/home/joe/tools/hadoop/hadoop-2.9.1
export SCALA_HOME=/home/joe/tools/scala/scala-2.11.12
export SPARK_HOME=/home/joe/tools/spark/spark-2.3.1-bin-without-hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

All of the build & execution commands are organized in the Makefile.

Execution
---------

1) Clone this repository to your local machine by using the following command in git bash:
	git clone https://github.ccs.neu.edu/ritikanair/CS6240-HW1-Spark.git

2) Open an instance of Command Prompt/Terminal here.

3) Navigate to inside the "TwitterSpark" folder:
	cd TwitterSpark
	
4) In the input folder, place the 'edges.csv' file from the Twitter dataset. I have not provided this
   file as part of this repo since it is a huge file.
   
5) Update the following variables in the Makefile to customize according to your setup:
    hadoop.root, 
    spark.root,
    hdfs.user.name, 
    aws.bucket.name	
    
6) To run in Standalone mode:
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
	
7) To run in Pseudo-Distributed mode: 
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running 

8) To run on AWS EMR Hadoop: 
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	make download-output-aws			-- after successful execution & termination

An output folder is created with the output files inside it in the same /TwitterSpark/ directory.
	
REFERENCES:
===========
1. MR-Demo and Spark-Demo code given by professor.
2. https://spark.apache.org/docs/1.3.1/api/java/org/apache/spark/rdd/RDD.html
