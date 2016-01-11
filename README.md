ZooFence
==============

ZooFence is a research prototype for consistent service partitioning. This project contains part of the source code we use for our SRDS [paper](https://drive.google.com/file/d/0BwFkGepvBDQobnJ2WWtDVjNXUlE) describing our principled approach.

This tutorial has been tested on `Ubuntu 12.04`.

# Dependencies #
* JDK 7
* ZooKeeper 3.4.5

# How to run the code #
We recommend that you set up a new project from existing sources in your favourite IDE (e.g., [Eclipse](http://stackoverflow.com/questions/2636201/how-to-create-a-project-from-existing-source-in-eclipse-and-then-find-it)).

# Configuration #
`resources/zkpartitioned.config` contains a sample configuration file.

The required configuration parameters are:

* `ZOOKEEPERS`: should contain `IP:PORT` pairs for different ZooKeeper instancess; each ZooKeeper instance represents a ZooFence partition.
* `ZKADMIN`: should contain an `IP:PORT` pair corresponding to the administrative ZooKeeper deployment, which is in charge of storing command queues.
* `FLATTENING_FACTOR`: controls when flattening operations are performed.
* `REDUCTION_FACTOR`: controls how many partitions to remove during a flattening operation.

# How to test ZooFence on localhost #
1. Deploy ZooKeeper instances

   Check the [official ZooKeeper documentation](https://zookeeper.apache.org/doc/r3.1.2/zookeeperAdmin.html#sc_singleAndDevSetup) for details on how to deploy and start ZooKeeper instances.

   To deploy multiple instances on localhost, use different "port" and "dir" parameters in the ZooKeeper configuration.
   e.g.,
   * `zookeeper0: ip:127.0.0.1, port:2181,  dir:/tmp/zookeeper  (zookeeper administrative)`
   * `zookeeper1: ip:127.0.0.1, port:12181, dir:/tmp/zookeeper1 (instance 1)`
   * `zookeeper2: ip:127.0.0.1, port:12182, dir:/tmp/zookeeper2 (instance 2)`
   * `zookeeper3: ip:127.0.0.1, port:12183, dir:/tmp/zookeeper3 (instance 3)`
   * `zookeeper4: ip:127.0.0.1, port:12184, dir:/tmp/zookeeper4 (instance 4)`

2. Start each ZooKeeper instance:

   e.g., To start zookeeper0:
   * `cd zookeeper0`
   * `./bin/zkServer.sh start`
   * `./bin/zkCli.sh -server localhost:2181`

3. Start the executor:

   * `java LogExecutor.java`
   * Make sure zkpartitioned.config is accessible.

4. Start a client:

   e.g., To start `SimpleTest.java`:
   * `java SimpleTest.java`
   * Make sure zkpartitioned.config is accessible.

# Contact #

Should you have any questions, please contact: `raluca.halalai@unine.ch`.