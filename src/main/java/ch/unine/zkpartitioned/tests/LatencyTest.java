package ch.unine.zkpartitioned.tests;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LatencyTest {

	public static void main(String[] args) throws IOException {
		
		if (args.length < 1) {
			System.err.println("Arg expected: \n" +
					"* zkp (to test latency for zookeeper partitioned) \n" +
					"* zk  (to test latency for zookeeper)");
			System.exit(0);
		}
		String zktype = args[0];
		
		Watcher watcher = new Watcher(){
			@Override
			public void process(WatchedEvent event) {
			}
			
		};
		
		if (zktype.equals("zkp")) {
			
			// generate node paths
			final int MAX_PATH = 10;
			String zkpPath = "";
			List<String> zkpsPaths = new ArrayList<String>();
			
			for (int i = 0; i < MAX_PATH; i++){
				zkpPath += "/node" + new Integer(i).toString();
				zkpsPaths.add(zkpPath);
			}
			
			long startTime, endTime;
			long zkpTime = 0;
			
			List<String> connectStrings = new ArrayList<String>();
			/*connectStrings.add("192.168.79.101:2181");
			connectStrings.add("192.168.79.102:2181");
			connectStrings.add("192.168.79.103:2181");
			connectStrings.add("192.168.79.105:2181");*/
			
			connectStrings.add("127.0.0.1:2181");
            connectStrings.add("127.0.0.1:2182");
//			connectStrings.add("127.0.0.1:12102");
//			connectStrings.add("127.0.0.1:12103");
//			connectStrings.add("127.0.0.1:12104");


            ZooKeeperPartitioned zkp = new ZooKeeperPartitioned("localhost:2181|localhost:2182",60000, watcher);
			// ZooKeeperPartitioned zkp = new ZooKeeperPartitioned(connectStrings,60000, watcher);
			zkp.setLeader(true);
			
            // create
            for (int i = 0; i < MAX_PATH; i++) {
                try {
                    zkpPath = zkpsPaths.get(i);
                    startTime = System.nanoTime();
                    zkp.create(zkpPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    endTime = System.nanoTime();
                    zkpTime = endTime - startTime;
                    System.out.println("Create " + zkpPath + " " + (i+1) + " " + zkpTime); // time in ns
                } catch (IllegalArgumentException | KeeperException
                        | InterruptedException e) {
                    e.printStackTrace();
                }

            }

			// exists
			try {
				Stat stat;
				for (int i = 0; i < MAX_PATH; i++) {
					zkpPath = zkpsPaths.get(i);
					startTime = System.nanoTime();
					stat = zkp.exists(zkpPath, true);
					endTime = System.nanoTime();
					zkpTime = endTime - startTime;
					
					System.out.println("Exists " + zkpPath + " " + (i+1) + " " + zkpTime); // time in ns
				}
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
			
			// get children
			try {
				List<String> children;
				
				for (int i = 0; i < MAX_PATH; i++) {
					zkpPath = zkpsPaths.get(i);
					startTime = System.nanoTime();
					children = zkp.getChildren(zkpPath, true);
					endTime = System.nanoTime();
					zkpTime = endTime - startTime;
					
					System.out.println("GetChildren " + zkpPath + " " + (i+1) + " " + zkpTime); // time in ns
				}
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
			
			// set data
			try {
				Stat stat; 
				
				for (int i = 0; i < MAX_PATH; i++) {
					zkpPath = zkpsPaths.get(i);
					startTime = System.nanoTime();
					stat = zkp.setData(zkpPath, "hello, world!".getBytes() , -1);
					endTime = System.nanoTime();
					zkpTime = endTime - startTime;
					
					System.out.println("SetData " + zkpPath + " " + (i+1) + " " + zkpTime); // time in ns
				}
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
			
			// get data
			try {
				for (int i = 0; i < MAX_PATH; i++) {
					zkpPath = zkpsPaths.get(i);
					startTime = System.nanoTime();
					zkp.getData(zkpPath, true, null);
					endTime = System.nanoTime();
					zkpTime = endTime - startTime;
					
					System.out.println("GetData " + zkpPath + " " + (i+1) + " " + zkpTime); // time in ns
				}
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
			
			// delete
			try {
				for (int i = MAX_PATH - 1; i >= 0; i--) {
					zkpPath = zkpsPaths.get(i);
					startTime = System.nanoTime();
					zkp.delete(zkpPath, -1);
					endTime = System.nanoTime();
					zkpTime = endTime - startTime;
					
					System.out.println("Delete " + zkpPath + " " + (i+1) + " " + zkpTime); // time in ns
				}
			} catch (InterruptedException | KeeperException e) {
				e.printStackTrace();
			}

            try {
                zkp.close();
            } catch (InterruptedException e) {
                e.printStackTrace();  // TODO: Customise this generated block
            }

        } else
			if (zktype.equals("zk")) {
				ZooKeeper zk0 = new ZooKeeper("127.0.0.1:2181", 1000, watcher);
				
				// generate node paths
				final int MAX_PATH = 10;
				String zkPath = "";
				List<String> zksPaths = new ArrayList<String>();
				
				for (int i = 0; i < MAX_PATH; i++){
					
					zkPath += "/node" + new Integer(i).toString();
					zksPaths.add(zkPath);
				}
				
				long startTime, endTime;
				long zkTime = 0;
				
				// create
				try {
					for (int i = 0; i < MAX_PATH; i++) {
						zkPath = zksPaths.get(i);
						startTime = System.nanoTime();
						zk0.create(zkPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						endTime = System.nanoTime();
						zkTime = endTime - startTime;
						
						System.out.println("Create " + zkPath + " " + (i+1) + " " + zkTime); // time in ns
					}	
				} catch (IllegalArgumentException | KeeperException
						| InterruptedException e) {
					e.printStackTrace();
				}
				
				// exists
				try {
					Stat stat;
					for (int i = 0; i < MAX_PATH; i++) {						
						zkPath = zksPaths.get(i);
						startTime = System.nanoTime();
						stat = zk0.exists(zkPath, true);
						endTime = System.nanoTime();
						zkTime = endTime - startTime;
						
						System.out.println("Exists " + zkPath + " " + (i+1) + " " + zkTime); // time in ns
					}
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
				
				// get children
				try {
					List<String> children;
					
					for (int i = 0; i < MAX_PATH; i++) {
						
						// zk
						zkPath = zksPaths.get(i);
						startTime = System.nanoTime();
						children = zk0.getChildren(zkPath, true);
						endTime = System.nanoTime();
						zkTime = endTime - startTime;
						
						System.out.println("GetChildren " + zkPath + " " + (i+1) + " " + zkTime); // time in ns
					}
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
				
				// set data
				try {
					Stat stat; 
					
					for (int i = 0; i < MAX_PATH; i++) {
						zkPath = zksPaths.get(i);
						startTime = System.nanoTime();
						stat = zk0.setData(zkPath, "hello, world!".getBytes() , -1);
						endTime = System.nanoTime();
						zkTime = endTime - startTime;
						
						System.out.println("SetData " + zkPath + " " + (i+1) + " " + zkTime); // time in ns
					}
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
				
				// get data
				try {
					for (int i = 0; i < MAX_PATH; i++) {
						zkPath = zksPaths.get(i);
						startTime = System.nanoTime();
						zk0.getData(zkPath, true, null);
						endTime = System.nanoTime();
						zkTime = endTime - startTime;
						
						System.out.println("GetData " + zkPath + " " + (i+1) + " " + zkTime); // time in ns
					}
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
				
				// delete
				try {
					for (int i = MAX_PATH - 1; i >= 0; i--) {
						zkPath = zksPaths.get(i);
						startTime = System.nanoTime();
						zk0.delete(zkPath, -1);
						endTime = System.nanoTime();
						zkTime = endTime - startTime;
						
						System.out.println("Delete " + zkPath + " " + (i+1) + " " + zkTime); // time in ns
					}
				} catch (InterruptedException | KeeperException e) {
					e.printStackTrace();
				}

                try {
                    zk0.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();  // TODO: Customise this generated block
                }

            } else {
				System.err.println("Arg expected: \n" +
						"* zkp (to test latency for zookeeper partitioned) \n" +
						"* zk  (to test latency for zookeeper)");
			}

        System.exit(0);

	}

}

