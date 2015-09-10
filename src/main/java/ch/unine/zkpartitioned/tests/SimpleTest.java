package ch.unine.zkpartitioned.tests;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleTest {

	public static void main(String[] args) throws IOException {
		Watcher watcher = new Watcher(){
			@Override
			public void process(WatchedEvent event) {
			}
			
		};
		
		List<String> connectStrings = new ArrayList<String>();
		connectStrings.add("127.0.0.1:2181");
		connectStrings.add("127.0.0.1:2182");

		ZooKeeperPartitioned zkp = new ZooKeeperPartitioned(connectStrings, 1000, watcher);

		try {
            zkp.create("/node0", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zkp.create("/node0/node1", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zkp.create("/node0/node2", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zkp.create("/node0/node3", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zkp.create("/node0/node4", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (IllegalArgumentException | KeeperException| InterruptedException e) {
			e.printStackTrace();
		}


        try {
            ZKUtil.deleteRecursive(zkp, "/node0");
        } catch (InterruptedException | KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            zkp.close();
        } catch (InterruptedException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }

        System.exit(0);
    }

}
