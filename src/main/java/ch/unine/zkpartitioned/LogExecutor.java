package ch.unine.zkpartitioned;

import ch.unine.zkpartitioned.Command.CmdType;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;

public class LogExecutor extends Thread {
	
	private Log log;
	private List<ZooKeeper> zookeepers;
	private MappingFunction mappingFunc;
	private int flatteningFactor;
    private Serializer serializer;

	public LogExecutor(Log log, List<ZooKeeper> zookeepers, MappingFunction mappingFunc, int flatteningFactor) {
		this.log = log;
		this.zookeepers = zookeepers;
		this.mappingFunc = mappingFunc;
		this.flatteningFactor = flatteningFactor;
        this.serializer = new Serializer();
	}
	
	@Override
	public void run() {		
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogExecutor.run");

		while (true) {

			LogEntry logEntry = log.nextEntry();
            assert log != null;
            assert logEntry != null;

			Object result = null;
            Command cmd = logEntry.getCmd();
            CmdType type = cmd.getType();
            String keySync = logEntry.getLogSync();

            if (keySync != null) { // a reference to the parent's log exists

                Log logSync = ZooKeeperPartitioned.logManager.getLog(keySync);
                logEntry.setLogSync(null);
                try {
                    result = logSync.add(logEntry);
                } catch (KeeperException e) {
                    e.printStackTrace();  // TODO: Customise this generated block
                } catch (InterruptedException e) {
                    e.printStackTrace();  // TODO: Customise this generated block
                }

            } else {

                // map to zookeepers and to logs
                List<ZooKeeper> zks = mappingFunc.getZks(logEntry.getLogKey());

                // execute command
                List<Object> results = new ArrayList<Object>(zks.size());
                synchronized (results) {
                    for (ZooKeeper zk : zks) {
                        CmdExecutor cmdExecutor = new CmdExecutor(cmd, zk, results);
                        cmdExecutor.start();
                    }

                    // wait for all zookeepers to complete the execution of the command
                    while (results.size() < zks.size()) {
                    try {
                            results.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

                if (results.get(0) != null && !(results.get(0) instanceof  Exception) ) {

                    switch(type) {
                    case EXISTS:
                        Stat stat = new Stat();
                        for (Object res : results) {
                            if (res != null) {
                                Stat statTmp = (Stat) res;

                                // get latest modified stat
                                if (stat.getMtime() < statTmp.getMtime()) {
                                    stat.setAversion(statTmp.getAversion());
                                    stat.setCtime(statTmp.getCtime());
                                    stat.setCversion(statTmp.getCversion());
                                    stat.setCzxid(statTmp.getCzxid());
                                    stat.setDataLength(statTmp.getDataLength());
                                    stat.setEphemeralOwner(statTmp.getEphemeralOwner());
                                    stat.setMtime(statTmp.getMtime());
                                    stat.setMzxid(statTmp.getMzxid());
                                    stat.setPzxid(statTmp.getPzxid());
                                    stat.setVersion(statTmp.getVersion());
                                }
                                // add num children
                                stat.setNumChildren(stat.getNumChildren() + statTmp.getNumChildren());
                            }
                        }

                        result = Serializer.serializeStat(stat);
                        break;
                    case GET_CHILDREN:
                        List<String> children = new ArrayList<String>();

                        for (Object res : results) {
                            List<String> childenTmp = (List<String>)res;

                            for (String s : childenTmp) {
                                if (children.contains(s) == false) {
                                    children.add(s);
                                }
                            }
                        }

                        result = children;
                        break;
                    case GET_DATA:
                        stat = new Stat();
                        byte[] data = null;

                        for (Object res : results) {
                            if (data == null)
                                data = ((Data)res).getData();

                            Stat statTmp = ((Data)res).getStat();

                            if (stat.getMtime() < statTmp.getMtime()) {
                                stat.setAversion(statTmp.getAversion());
                                stat.setCtime(statTmp.getCtime());
                                stat.setCversion(statTmp.getCversion());
                                stat.setCzxid(statTmp.getCzxid());
                                stat.setDataLength(statTmp.getDataLength());
                                stat.setEphemeralOwner(statTmp.getEphemeralOwner());
                                stat.setMtime(statTmp.getMtime());
                                stat.setMzxid(statTmp.getMzxid());
                                stat.setPzxid(statTmp.getPzxid());
                                stat.setVersion(statTmp.getVersion());
                            }

                            // add children number
                            stat.setNumChildren(stat.getNumChildren() + statTmp.getNumChildren());
                        }

                        result = Serializer.serializeData(new Data(data, stat));
                        break;
                    case SET_DATA:
                        stat = new Stat();
                        for (Object res : results) {
                            Stat statTmp = (Stat) res;

                            if (stat.getMtime() < statTmp.getMtime()) {
                                stat.setAversion(statTmp.getAversion());
                                stat.setCtime(statTmp.getCtime());
                                stat.setCversion(statTmp.getCversion());
                                stat.setCzxid(statTmp.getCzxid());
                                stat.setDataLength(statTmp.getDataLength());
                                stat.setEphemeralOwner(statTmp.getEphemeralOwner());
                                stat.setMtime(statTmp.getMtime());
                                stat.setMzxid(statTmp.getMzxid());
                                stat.setPzxid(statTmp.getPzxid());
                                stat.setVersion(statTmp.getVersion());
                            }

                            // add children number
                            stat.setNumChildren(stat.getNumChildren() + statTmp.getNumChildren());
                        }

                        result = Serializer.serializeStat(stat);
                        break;

                    default:
                        result = results.get(0);
                        break;
                    }

                } else {
                    result = results.get(0);
                }

            } // keysync == null

            logEntry.setResult(serializer.serialize(result));
            try {
                log.updateEntry(logEntry);
            } catch (KeeperException e) {
                e.printStackTrace();  // TODO: Customise this generated block
            } catch (InterruptedException e) {
                e.printStackTrace();  // TODO: Customise this generated block
            }


        }
	}
}