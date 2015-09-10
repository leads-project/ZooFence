package ch.unine.zkpartitioned;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class CmdExists extends Command {

	private static final long serialVersionUID = 1L;
	private Object watcher;
	private String watcherType;

	public CmdExists(String id, String path, Watcher watcher) {
		super(id, path, CmdType.EXISTS);
		this.watcher = watcher;
		watcherType = "Watcher";
	}
	
	public CmdExists(String id, String path, boolean watcher) {
		super(id, path, CmdType.EXISTS);
		this.watcher = watcher;
		watcherType = "boolean";
	}
	
	@Override
	public Object execute(ZooKeeper zk) throws KeeperException, InterruptedException {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Exists.execute");
		
		Stat stat = null;
		if (watcherType.equals("Watcher")) {
			Watcher w  = null;
			if (watcher != null)
				w = (Watcher) watcher;
			stat = zk.exists(path, w);
		} else {
			boolean w = (boolean) watcher;
			stat = zk.exists(path, w);
		}
		
		return stat;
	}
}
