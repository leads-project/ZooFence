package ch.unine.zkpartitioned.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;

public class ThroughputTest {
	public static Integer msgs;

	public static void main(String[] args) throws IOException {

		if (args.length < 3) {
			System.err.println("Args expected: \n" +
					"* operation (e.g., create)\n" +
					"* zk or zkp \n" +
					"* leader or follower");
			System.exit(0);
		}
		
		String operation = args[0];
		String zktype = args[1];
		String role = args[2];
		
		Watcher watcher = new Watcher(){
			public void process(WatchedEvent event) {
			}
		};
		
	    msgs = 0;
		final int timeout = 60;

		// a thread that kills the process and exits after timeout
    	Thread timer = new Thread() {
    	    public void run() {
    	        try {

    	            Thread.sleep(timeout * 1000);
    	            System.out.println(msgs);
    	            
    	        } catch(InterruptedException v) {
    	        }
    	    }  
    	};
		
    	if (zktype.equals("zkp")) {
    		List<String> connectStrings = new ArrayList<String>();
    		connectStrings.add("192.168.79.101:2181");
    		connectStrings.add("192.168.79.102:2181");
    		connectStrings.add("192.168.79.103:2181");
    		connectStrings.add("192.168.79.104:2181");
    		/*
    		connectStrings.add("127.0.0.1:12101");
    		connectStrings.add("127.0.0.1:12102");
    		connectStrings.add("127.0.0.1:12103");
    		connectStrings.add("127.0.0.1:12104");
    		*/
    		
    		ZooKeeperPartitioned zkp = new ZooKeeperPartitioned(connectStrings, 1000, watcher);
    		if (role.equals("leader"))
    			zkp.setLeader(true);
    		
    		// create prefix path
    		String path = "/root";
    		try {
				if (zkp.exists(path, false) == null)
					zkp.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (IllegalArgumentException | InterruptedException e) {
				e.printStackTrace();
			} catch (KeeperException e) {}
    		
    		switch(operation) {
	    		case "create":       	    
					timer.start();
					for (;;) {
						synchronized (msgs) {
							try {
								zkp.create(path + "/node" + msgs, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
								msgs++;
							} catch (IllegalArgumentException | KeeperException | InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
				default:
					System.out.println("TODO Unimplemented operation!");
					break;
    		}

    	} else {
    		//ZooKeeper zk0 = new ZooKeeper("127.0.0.1:12100", 1000, watcher);
    		ZooKeeper zk0 = new ZooKeeper("192.168.79.100:2181", 1000, watcher);
    		
    		// create prefix path
    		String path = "/root";
    		try {
				if (zk0.exists(path, false) == null)
					zk0.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (IllegalArgumentException | InterruptedException e) {
				e.printStackTrace();
			} catch (KeeperException e) {}
    		
    		switch(operation) {
	    		case "create":       	    
					timer.start();
					for (;;) {
						synchronized (msgs) {
							try {
								zk0.create(path + "/node" + msgs, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
								msgs++;
							} catch (IllegalArgumentException | KeeperException | InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
				
	    		default:
					System.out.println("TODO Unimplemented operation!");
					break;
    		}
    	}

	}
}
