package ch.unine.zkpartitioned;

import java.io.Serializable;

public class LogEntry implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private Command cmd;		// the command
    private byte[] result;     // the result
	private String logPath;		// the log path
	private String logKey;		// the log key
	private String logSync;		// the key of the log to keep in sync with
	private String clientId;    // the if od the client

	public LogEntry(Command cmd) {
		this.cmd = cmd;
		setClientId(ZooKeeperPartitioned.clientId);
	}

	public Command getCmd() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogEntry.getCmd");
			 
		return cmd;
	}

	public String getLogPath() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogEntry.getLogPath");
		
		return logPath;
	}

	public void setPath(String logPath) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogEntry.setPath");
		
		this.logPath = logPath;
	}

	public String getLogSync() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogEntry.getLogSync");
		
		return logSync;
	}

	public void setLogSync(String logSync) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogEntry.setLogSync");
		
		this.logSync = logSync;
	}

	public String getLogKey() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogEntry.getLogKey");
		
		return logKey;
	}

	public void setLogKey(String logKey) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogEntry.setLogKey");
		
		this.logKey = logKey;
	}
	
	public String getClientId() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogEntry.getClientId");
		
		return clientId;
	}

	public void setClientId(String clientId) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("LogEntry.setClientId");
		
		this.clientId = clientId;
	}

    public void setResult(byte[] result){
        if (ZooKeeperPartitioned.logger.isTraceEnabled())
            ZooKeeperPartitioned.logger.trace("LogEntry.setResult");
        this.result = result;
    }

    public byte[] getResult(){
        return result;
    }
}

