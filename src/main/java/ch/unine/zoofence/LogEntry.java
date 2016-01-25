package ch.unine.zoofence;

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
		setClientId(ZooFence.clientId);
	}

	public Command getCmd() {
		if (ZooFence.logger.isTraceEnabled())
			 ZooFence.logger.trace("LogEntry.getCmd");
			 
		return cmd;
	}

	public String getLogPath() {
		if (ZooFence.logger.isTraceEnabled())
			 ZooFence.logger.trace("LogEntry.getLogPath");
		
		return logPath;
	}

	public void setPath(String logPath) {
		if (ZooFence.logger.isTraceEnabled())
			 ZooFence.logger.trace("LogEntry.setPath");
		
		this.logPath = logPath;
	}

	public String getLogSync() {
		if (ZooFence.logger.isTraceEnabled())
			 ZooFence.logger.trace("LogEntry.getLogSync");
		
		return logSync;
	}

	public void setLogSync(String logSync) {
		if (ZooFence.logger.isTraceEnabled())
			 ZooFence.logger.trace("LogEntry.setLogSync");
		
		this.logSync = logSync;
	}

	public String getLogKey() {
		if (ZooFence.logger.isTraceEnabled())
			 ZooFence.logger.trace("LogEntry.getLogKey");
		
		return logKey;
	}

	public void setLogKey(String logKey) {
		if (ZooFence.logger.isTraceEnabled())
			 ZooFence.logger.trace("LogEntry.setLogKey");
		
		this.logKey = logKey;
	}
	
	public String getClientId() {
		if (ZooFence.logger.isTraceEnabled())
			 ZooFence.logger.trace("LogEntry.getClientId");
		
		return clientId;
	}

	public void setClientId(String clientId) {
		if (ZooFence.logger.isTraceEnabled())
			 ZooFence.logger.trace("LogEntry.setClientId");
		
		this.clientId = clientId;
	}

    public void setResult(byte[] result){
        if (ZooFence.logger.isTraceEnabled())
            ZooFence.logger.trace("LogEntry.setResult");
        this.result = result;
    }

    public byte[] getResult(){
        return result;
    }
}

