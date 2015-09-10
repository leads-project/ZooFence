package ch.unine.zkpartitioned;

import java.util.HashMap;
import java.util.Map;


public class LogManager {
	
	private static LogManager instance = null;
	private Map<String, Log> logs;

	private LogManager() {
		logs = new HashMap<String, Log>();
	}
	
	public static synchronized LogManager getInstance() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogManager.getInstance");
		
		if (instance == null) {
			synchronized (LogManager .class){
				if (instance == null) {
					instance = new LogManager();
				}
			}
		}
		return instance;
	}

	public synchronized Log getLog(String key) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogManager.getLog");
		
		if (key.length() == 1)
			return null;
		
		Log log = logs.get(key);
		
		if (log == null) {
			log = new Log(ZooKeeperPartitioned.connectionStrings.get(0),ZooKeeperPartitioned.sessionTimeout,key);
			logs.put(key, log);
			// TODO add self as leader of this log
			// TODO if log leader, start log executor
			if (ZooKeeperPartitioned.leader) {
				LogExecutor logExecutor = new LogExecutor(log, ZooKeeperPartitioned.zookeepers, ZooKeeperPartitioned.mappingFunc, Configuration.flatteningFactor);
				logExecutor.start();
			}
		}
		
		return log;
	}
}
