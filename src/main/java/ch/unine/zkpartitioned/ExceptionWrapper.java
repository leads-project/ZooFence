package ch.unine.zkpartitioned;

import org.apache.zookeeper.KeeperException;

public class ExceptionWrapper {
	
	public enum ExceptionType {KeeperException, InterruptedException, IllegalArgumentException};
	private Exception exception;
	private ExceptionType type;
	private int code;

	public ExceptionWrapper(Exception exception) {
		this.exception = exception;
		
		String exceptionClass = exception.getClass().getName();
		
		if (exceptionClass.contains("KeeperException")) {
			code = ((KeeperException)exception).code().intValue();
			type = ExceptionType.KeeperException;
			return;
		}
		
		switch (exceptionClass) {
		case "InterruptedException":
			type = ExceptionType.InterruptedException;
			break;
		case "IllegalArgumentException":
			type = ExceptionType.IllegalArgumentException;
			break;
		}
	}

	public void throwException() throws KeeperException, InterruptedException, IllegalArgumentException {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("ExceptionWrapper.throwException");
		
		switch(type) {
		case KeeperException:
			throw (KeeperException.create(code, ((KeeperException)exception).getPath()));
		case InterruptedException:
			throw (InterruptedException) exception;
		case IllegalArgumentException:
			throw (IllegalArgumentException) exception;
		}
	}
}
