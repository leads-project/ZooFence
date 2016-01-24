ZooFence
==============

ZooFence is a research prototype for consistent service partitioning. This project contains part of the source code we use for our SRDS [paper](https://drive.google.com/file/d/0BwFkGepvBDQobnJ2WWtDVjNXUlE) describing our principled approach.

This tutorial has been tested on `Ubuntu 12.04`.

# Dependencies #
* JDK >= 7
* Maven >= 3.x
* ZooKeeper 3.4.6
* Menagerie 1.2-SNAPSHOT (git clone https://github.com/otrack/menagerie.git && cd menagerie && mvn install -DskipTests)

# How to run the code #
We recommend that you set up a new project from existing sources in your favourite IDE (e.g., [Eclipse](http://stackoverflow.com/questions/2636201/how-to-create-a-project-from-existing-source-in-eclipse-and-then-find-it)).

# Configuration #
`resources/zkpartitioned.config` contains a sample configuration file.

The required configuration parameters are:
* `ZOOKEEPERS`: should contain `IP:PORT` pairs for different ZooKeeper instances; each ZooKeeper instance represents a ZooFence partition.
* `ZKADMIN`: should contain an `IP:PORT` pair corresponding to the administrative ZooKeeper deployment, which is in charge of storing command queues.
* `FLATTENING_FACTOR`: controls when flattening operations are performed.
* `REDUCTION_FACTOR`: controls how many partitions to remove during a flattening operation.

# How to test ZooFence on localhost #
1. Deploy ZooKeeper instances
   Check the [official ZooKeeper documentation](https://zookeeper.apache.org/doc/r3.1.2/zookeeperAdmin.html#sc_singleAndDevSetup) for details on how to deploy and start ZooKeeper instances.
   To deploy multiple instances on localhost, use different "port" and "dir" parameters in the ZooKeeper configuration.
   e.g.,
   * `zookeeper1: ip:127.0.0.1, port:2181, dir:/tmp/zookeeper0 (instance 1)`
   * `zookeeper2: ip:127.0.0.1, port:2182, dir:/tmp/zookeeper1 (instance 2)`

Note that the instructions below takes these requirements into account. 

2. Configure and start both ZK instances
```bash
   wget http://mirror.switch.ch/mirror/apache/dist/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
   tar zxvf zookeeper-3.4.6.tar.gz
   mv zookeeper-3.4.6 zookeeper0
   cp -r zookeeper0 zookeeper1
   cd zookeeper0`
   cat conf/zoo_sample.cfg | sed s/\\/tmp\\/zookeeper/\\/tmp\\/zookeeper0/g > conf/zoo.cfg
   ./bin/zkServer.sh start
   cd ../zookeper1
   cat conf/zoo_sample.cfg | sed s/2181/2182/g | sed s/\\/tmp\\/zookeeper/\\/tmp\\/zookeeper1/g > conf/zoo.cfg
   ./bin/zkServer.sh start
```

3. The class [SimpleTest](https://github.com/leads-project/ZooFence/blob/master/src/test/java/ch/unine/zoofence/SimpleTest.java) acts as simple client for ZooFence. You can launch it with:

   * `mvn test -Dtest=SimpleTest`

# Contact #

Should you have any questions, please contact `raluca.halalai@unine.ch` or `pierre.sutra@telecom-sudparis.eu`.
