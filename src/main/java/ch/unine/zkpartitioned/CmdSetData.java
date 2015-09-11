package ch.unine.zkpartitioned;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class CmdSetData extends Command {

	private static final long serialVersionUID = 1L;
	private byte[] data;
	private int version;
	
	public CmdSetData(String id, String path, byte[] data, int version) {
		super(id, path, CmdType.SET_DATA);
		this.data = data;
		this.version = version;
	}
	
	public Object execute(ZooKeeper zk) throws KeeperException, InterruptedException {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("CmdSetData.execute");
		
		Stat stat = zk.setData(path, data, version);
		
		if (stat != null)
			return stat;
		
		return null;
	}

	public byte[] getData() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("CmdSetData.getData");
		
		return data;
	}

	public void setData(byte[] data) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("CmdSetData.setData");
		
		this.data = data;
	}

	public int getVersion() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("SetData.getVersion");
		
		return version;
	}

	public void setVersion(int version) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("SetData.setVersion");
		
		this.version = version;
	}
}
