package ch.unine.zkpartitioned;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class CmdDelete extends Command {

	private static final long serialVersionUID = 1L;
	private int version;
	
	public CmdDelete(String id, String path, int version) {
		super(id, path, CmdType.DELETE);
		this.version = version;
	}
	
	@Override
	public Object execute(ZooKeeper zk) throws InterruptedException, KeeperException {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("CmdDelete.execute");
		
		zk.delete(path, version);

		return null;
	}

	public int getVersion() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("CmdDelete.getVersion");
		
		return version;
	}

	public void setVersion(int version) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("CmdDelete.setVersion");
			 
		this.version = version;
	}
}
