package ch.unine.zkpartitioned;

import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.abs;

public class MappingFunction {
	private List<ZooKeeper> zookeepers;
	private int flatteningFactor; /* when path length divisible with flattening factor, remove zookeepers */
	private int reductionFactor;  /* number of zookeepers to remove */
	
	public MappingFunction(List<ZooKeeper> zookeepers, int flatteningFactor, int reductionFactor) {
		this.zookeepers = zookeepers;
		this.flatteningFactor = flatteningFactor;
		this.reductionFactor = reductionFactor;
	}
	
	// builds list of zks
	public void computePartitions(String path, List<ZooKeeper> zks) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("MappingFunction.computePartitions");

		String[] pathTokens = path.split("/");
		int pathTokensLen = pathTokens.length - 1;
		
		if (pathTokensLen > 1) {
				String parentPath = "";
				
				for (int i = 1 ; i < pathTokensLen; i++) { // skip first token since it's space
					parentPath += "/".concat(pathTokens[i]);
				}
				
				computePartitions(parentPath,zks);
				
				if (pathTokensLen % flatteningFactor == 0) {
					int reductionCount = 0;
					int indexToRemove;
					
					while(zks.size() > 1 && reductionCount < reductionFactor) {
						indexToRemove = path.hashCode() % zks.size();
						zks.remove(abs(indexToRemove));
						reductionCount++;
					}
				}
			}
	}

    public List<ZooKeeper> getZks(String key) {
        if (ZooKeeperPartitioned.logger.isTraceEnabled())
            ZooKeeperPartitioned.logger.trace("MappingFunction.getZks");

        List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
        for (char ch : key.toCharArray()) {
            int index = Character.getNumericValue(ch);
            zks.add(zookeepers.get(index));
        }
        return zks;
    }

    public void map(Command cmd, List<ZooKeeper> zks, List<String> logKeys) {
        String path = cmd.getPath();
        String parentPath = cmd.getParentPath();
        if(path.contains("/ledgers")){
            if(path.equals("/ledgers")){
                zks.addAll(zookeepers);
                logKeys.add(computeKey(zks));
            }else if(parentPath.equals("/ledgers")){
                int hash = abs(path.hashCode());
                int zkNumber = hash%zookeepers.size();
                zks.add(zookeepers.get(zkNumber));
                logKeys.add(((Integer)zkNumber).toString());
            }else{
                int hash = abs(parentPath.hashCode());
                int zkNumber = hash%zookeepers.size();
                zks.add(zookeepers.get(zkNumber));
                logKeys.add(((Integer)zkNumber).toString());
            }
        }else if(path.contains("/coord")){
            zks.add(zookeepers.get(0));
            logKeys.add(((Integer)0).toString());
        }else{
            regularMap(cmd, zks, logKeys);
        }
        System.out.println("MAPPING "+cmd+" >> "+logKeys);
    }


 //    public void map(Command cmd, List<ZooKeeper> zks, List<String> logKeys) {
//        String root = cmd.getRootPath();
//        int hash = abs(root.hashCode());
//        int zkNumber = hash%zookeepers.size();
//        zks.add(zookeepers.get(zkNumber));
//        logKeys.add(((Integer)zkNumber).toString());
//    }


//	public void map(Command cmd, List<ZooKeeper> zks, List<String> logKeys) {
//		String path = cmd.getPath();
//		char digit = 0;
//
//		for (int i = 0 ; i < path.length() && digit == 0; i++) {
//			if (Character.isDigit(path.charAt(i))) {
//				digit = path.charAt(i);
//			}
//		}
//
//		if (Character.isDigit(digit)) {
//
//			int zkNumber = digit - '0';
//			zks.add(zookeepers.get(zkNumber % zookeepers.size()));
//
//			logKeys.add(((Integer)zkNumber).toString());
//		} else {
//			String key = "";
//			for (ZooKeeper z : zookeepers) {
//				zks.add(z);
//				key += ((Integer)zookeepers.indexOf(z)).toString();
//			}
//			logKeys.add(key);
//		}
//	}

    //
    // PRIVATE INTERFACE
    //

    // builds log key
    private String computeKey(List<ZooKeeper> zks) {
        if (ZooKeeperPartitioned.logger.isTraceEnabled())
            ZooKeeperPartitioned.logger.trace("MappingFunction.computeKey");

        List<Integer> keys = new ArrayList<Integer>();
        String logKey = "";

        for (ZooKeeper zk : zks) {
            logKey += zookeepers.indexOf(zk);
        }

        return logKey;
    }

    private void regularMap(Command cmd, List<ZooKeeper> zks, List<String> logKeys) {
        if (ZooKeeperPartitioned.logger.isTraceEnabled())
            ZooKeeperPartitioned.logger.trace("MappingFunction.map");

        // generate partitions
        List<ZooKeeper> zksClone = new ArrayList<ZooKeeper>(zookeepers);
        computePartitions(cmd.getPath(), zksClone);
        for (ZooKeeper zk : zksClone) {
            zks.add(zk);
        }

        // log key
        int pathLen = cmd.getPathLength();
        //if (zks.size() > 1)
        logKeys.add(computeKey(zksClone));

        List<String> r = new ArrayList<String>();
        for(ZooKeeper zk : zks){
            r.add(Long.toString(zk.getSessionId()));
        }

        // parent log key
        if (cmd.getType().equals(Command.CmdType.CREATE) && cmd.getPathLength() % flatteningFactor == 0) {
            String parentPath = cmd.getParentPath();

            if (!parentPath.equals("/")) {
                zksClone = new ArrayList<ZooKeeper>(zookeepers);
                computePartitions(parentPath, zksClone);

                if (zksClone.size() > 1)
                    logKeys.add(computeKey(zksClone));
            }
        }
    }

}
