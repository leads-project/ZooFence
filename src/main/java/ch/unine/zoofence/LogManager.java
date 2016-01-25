package ch.unine.zoofence;

import java.util.HashMap;
import java.util.Map;


public class LogManager {
	
	private static LogManager instance = null;
	private Map<String, Log> logs;

	private LogManager() {
		logs = new HashMap<String, Log>();
	}
	
	public static synchronized LogManager getInstance() {
		if (ZooFence.logger.isTraceEnabled())
			 ZooFence.logger.trace("LogManager.getInstance");
		
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
		if (ZooFence.logger.isTraceEnabled())
			 ZooFence.logger.trace("LogManager.getLog");
		
		if (key.length() == 1)
			return null;
		
		Log log = logs.get(key);
		
		if (log == null) {
			log = new Log(ZooFence.connectionStrings.get(0), ZooFence.sessionTimeout,key);
			logs.put(key, log);
			// TODO add self as leader of this log
			// TODO if log leader, start log executor
			if (ZooFence.leader) {
				LogExecutor logExecutor = new LogExecutor(log, ZooFence.zookeepers, ZooFence.mappingFunc, Configuration.flatteningFactor);
				logExecutor.start();
			}
		}
		
		return log;
	}
}
