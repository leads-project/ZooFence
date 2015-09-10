package ch.unine.zkpartitioned;
import org.apache.zookeeper.data.Stat;

public class Data {
		private byte[] data;
		private Stat stat;
		
		public Data(byte[] data, Stat stat) {
			this.data = data;
			this.stat = stat;
		}

		public byte[] getData() {
			if (ZooKeeperPartitioned.logger.isTraceEnabled())
				 ZooKeeperPartitioned.logger.trace("Data.getData");
			
			return data;
		}

		public void setData(byte[] data) {
			if (ZooKeeperPartitioned.logger.isTraceEnabled())
				 ZooKeeperPartitioned.logger.trace("Data.setData");
			
			this.data = data;
		}

		public Stat getStat() {
			if (ZooKeeperPartitioned.logger.isTraceEnabled())
				 ZooKeeperPartitioned.logger.trace("Data.getStat");
			
			return stat;
		}

		public void setStat(Stat stat) {
			if (ZooKeeperPartitioned.logger.isTraceEnabled())
				 ZooKeeperPartitioned.logger.trace("Data.setStat");
			
			this.stat = stat;
		}
	}
