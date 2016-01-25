package ch.unine.zoofence;
import org.apache.zookeeper.data.Stat;

public class Data {
		private byte[] data;
		private Stat stat;
		
		public Data(byte[] data, Stat stat) {
			this.data = data;
			this.stat = stat;
		}

		public byte[] getData() {
			if (ZooFence.logger.isTraceEnabled())
				 ZooFence.logger.trace("Data.getData");
			
			return data;
		}

		public void setData(byte[] data) {
			if (ZooFence.logger.isTraceEnabled())
				 ZooFence.logger.trace("Data.setData");
			
			this.data = data;
		}

		public Stat getStat() {
			if (ZooFence.logger.isTraceEnabled())
				 ZooFence.logger.trace("Data.getStat");
			
			return stat;
		}

		public void setStat(Stat stat) {
			if (ZooFence.logger.isTraceEnabled())
				 ZooFence.logger.trace("Data.setStat");
			
			this.stat = stat;
		}
	}
