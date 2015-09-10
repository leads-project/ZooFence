package ch.unine.zkpartitioned;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.Serializable;

public abstract class Command implements Serializable {

	protected static final long serialVersionUID = 1L;
	
	public enum CmdType {CREATE, DELETE, EXISTS, GET_CHILDREN, GET_DATA, SET_DATA};
	
	protected String id;
	protected String path;
	protected CmdType type;

	public Command(String id, String path, CmdType type) {
		this.id = id;
		this.path = path;
		this.type = type;
	}
	
	public abstract Object execute(ZooKeeper zk) throws KeeperException, InterruptedException;

	public String getPath() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Command.getPath");
		
		return path;
	}

	public CmdType getType() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Command.getType");
		
		return type;
	}
	
	public String getId() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Command.getId");
		
		return id;
	}

    public String getRootPath(){
        if (ZooKeeperPartitioned.logger.isTraceEnabled())
            ZooKeeperPartitioned.logger.trace("Command.getRootPath");
        return "/" + path.split("/")[1];
    }
	
	public String getParentPath() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Command.getParentPath");
		
		String parentPath = "";
		
		String[] pathTokens = path.split("/");
		for (int i = 1 ; i < pathTokens.length-1; i++) {
			parentPath += "/".concat(pathTokens[i]);
		}
		
		if (parentPath.equals(""))
			parentPath = "/";

		return parentPath;
	}
	
	public int getPathLength() {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Command.getPathLength");
		
		String[] pathTokens = path.split("/");
		return (pathTokens.length - 1);
	}

    @Override
    public boolean equals(Object o){
        if(!(o instanceof Command))
            return false;
        return ((Command)o).id.equals(this.id);
    }

    @Override
    public String toString(){
        return "C"+getType().toString()+getPath();
    }

}
