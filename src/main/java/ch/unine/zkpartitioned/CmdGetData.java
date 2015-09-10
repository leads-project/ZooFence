package ch.unine.zkpartitioned;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class CmdGetData extends Command {

	private static final long serialVersionUID = 1L;
	private Object watcher;
	private String watcherType;
	private byte[] statSerialized;

	public CmdGetData(String id, String path, Watcher watcher, Stat stat) {
		super(id, path, CmdType.GET_DATA);
		this.watcher = watcher;
		watcherType = "Watcher";
		statSerialized = Serializer.serializeStat(stat);
	}
	
	public CmdGetData(String id, String path, boolean watcher, Stat stat) {
		super(id, path, CmdType.GET_DATA);
		this.watcher = watcher;
		watcherType = "boolean";
		statSerialized = Serializer.serializeStat(stat);
	}
	
	// return the data and the stat of the node of the given path
	@Override
	public Object execute(ZooKeeper zk) throws KeeperException, InterruptedException {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("GetData.execute");
		
		Stat stat = Serializer.deserializeStat(statSerialized);
		
		byte[] data = null;
		if (watcherType.equals("Watcher")) {
			Watcher w = (Watcher) watcher;
			data = zk.getData(path, w, stat);
		} else {			
			boolean w = (boolean) watcher;
			data = zk.getData(path, w, stat);
		}
		
		return new Data(data, stat);
	}
	
	public Stat getStat() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("CmdGetData.getStat");
			 
		return Serializer.deserializeStat(statSerialized);
	}
}
