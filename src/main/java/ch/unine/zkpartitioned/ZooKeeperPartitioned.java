package ch.unine.zkpartitioned;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class ZooKeeperPartitioned extends ZooKeeper{

    public static final String ZKP_SEPARATOR = "|";

	/* unique client id */
	public static String clientId;
	
	/* log4j throughout the zkpartitioned code */
	final static Logger logger = LoggerFactory.getLogger(ZooKeeperPartitioned.class);
	
	/* increasing number that uniquely identifies a command; initialized to 0 in constructor */
	private volatile int cmdNumber;
	
	/* list of zk instances, i.e., partitions */
	public static List<ZooKeeper> zookeepers;
    public static List<String> connectionStrings;
    public static int sessionTimeout;


	/* function that maps commands to a set of zk instances (partitions) */
	public static MappingFunction mappingFunc;

	/* in charge with returning the right log */
	public static LogManager logManager;

	/* if true, this process will create a LogExecutor thread; the creator of a log is its leader */
	public static boolean leader;


    /**
     * Constructor, the configuration stores the connection strings.
     * @param sessionTimeout - same as for ZooKeeper
     * @param watcher - same as for ZooKeeper
     * @throws IOException
     */
    public ZooKeeperPartitioned (int sessionTimeout, Watcher watcher) throws IOException {
        super("",0,null);
        StringTokenizer st = new StringTokenizer(Configuration.zksConnectStrings,ZKP_SEPARATOR);
        List<String> connectStrings = new ArrayList<String>();
        while(st.hasMoreElements())
            connectStrings.add((String)st.nextElement());
        init(connectStrings,sessionTimeout,watcher);
    }

    /**
     * Constructor with explicit connection string.
     * @param fullConnectString - list of instances separated  by '/' , the admin is the first one..
     * @param sessionTimeout - same as for ZooKeeper
     * @param watcher - same as for ZooKeeper
     * @throws IOException
     */
    public ZooKeeperPartitioned (String fullConnectString, int sessionTimeout, Watcher watcher) throws IOException {
        super("",0,null);
        StringTokenizer st = new StringTokenizer(fullConnectString,ZKP_SEPARATOR);
        List<String> connectStrings = new ArrayList<String>();
        while(st.hasMoreElements())
            connectStrings.add((String)st.nextElement());
        init(connectStrings,sessionTimeout,watcher);

    }


    /**
	 * Constructor with explicit connection string.
	 * @param connectStrings - list of host:ip connection strings, the admin is the first one.
	 * @param sessionTimeout - same as for ZooKeeper
	 * @param watcher - same as for ZooKeeper
	 * @throws IOException
	 */
	public ZooKeeperPartitioned (List<String> connectStrings, int sessionTimeout, Watcher watcher) throws IOException {
        super("",0,null);
        init(connectStrings,sessionTimeout,watcher);
	}

    private void init(List<String> connectStrings, int sessionTimeout, Watcher watcher) throws IOException {

        /* assign unique id to this process */
        clientId = assignClientId();

		/* initialize unique cmd number */
        cmdNumber = 0;

        /* initialize array of zk instances (the "partitions") */
        zookeepers = new ArrayList<ZooKeeper>();
        connectionStrings = new ArrayList<String>();
        this.sessionTimeout = sessionTimeout;
        for (String conn : connectStrings) {
            ZooKeeper zk = new ZooKeeper(conn, sessionTimeout, watcher);
            zookeepers.add(zk);
            connectionStrings.add(conn);
        }

		/* initialize mapping function */
        mappingFunc = new MappingFunction(zookeepers, Configuration.flatteningFactor, Configuration.reductionFactor);

		/* initialize log manager */
        logManager = LogManager.getInstance();

        // TODO
        setLeader(true);

        try {
            super.close();
        } catch (InterruptedException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }
    }
	
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) 
			throws IllegalArgumentException, KeeperException, InterruptedException {
		
		if (logger.isTraceEnabled())
			logger.trace("CREATE operation");
		
		Command cmd = new CmdCreate(assignCmdId(), path, data, acl, createMode);
		Object result = null;
		
		/* map to zks and log keys */
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		/* trivial cmd */
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
		} 
		/* non-trivial cmd */ 
		else {
			/* TODO
			 * not sure about get(0) and get(1); if the size is fixed, use array! 
			 * but array cannot be passed by reference, so it has to be returned
			 */
			Log log = logManager.getLog(logKeys.get(0));
			
			/* handle case where child is not replicated (no log), but parent is */
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd);
			logEntry.setLogKey(logKeys.get(0));
			
			/* TODO: replace length with something else after changing tokenization */
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			
			result = log.add(logEntry);
		}
		
		String actualPath = null;
		if (result != null && result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		else {
			actualPath = (String)result;
		}
		
		if (logger.isTraceEnabled())
			logger.trace("CREATE END");
			
		return actualPath;
	}

    public void create(final String path, byte data[], List<ACL> acl,
                       CreateMode createMode,  AsyncCallback.StringCallback cb, Object ctx)
    {
        String result = "";
        try {
            result = create(path,data,acl,createMode);
        } catch (Exception e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }
        cb.processResult(0,path,ctx,result);
    }

    @Override
	public void delete(String path, int version) throws InterruptedException, KeeperException {
		
		if (logger.isTraceEnabled())
			logger.trace("DELETE START");
		
		Command cmd = new CmdDelete(assignCmdId(), path, version);
		Object result = null;
		
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		// trivial cmd
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			
			result = log.add(logEntry);
		}
		
		if (result != null && result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}

		if (logger.isTraceEnabled())
			logger.trace("DELETE END");
	}

    @Override
	public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
		
		if (logger.isTraceEnabled())
			logger.trace("EXISTS START");
		
		Command cmd = new CmdExists(assignCmdId(), path, watcher);
		Object result = null;
				
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		// trivial cmd
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			
			result = log.add(logEntry);
		}
		
		Stat stat = null;
		if (result != null) {
			if (result.toString().contains("Exception")) {
				ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
				exception.throwException();
			} else {
                if(!(result instanceof Stat))
                    stat = (Stat)Serializer.deserializeStat((byte[]) result);
                else
                    stat = (Stat) result;
			}
		}
		
		if (logger.isTraceEnabled())
			logger.trace("EXISTS END");
		
		return stat;
	}

    @Override
	public Stat exists(String path, boolean watcher) throws KeeperException, InterruptedException {
		
		if (logger.isTraceEnabled())
			logger.trace("EXISTS START");
		
		Command cmd = new CmdExists(assignCmdId(), path, watcher);
		Object result = null;
		boolean trivial = false;
		
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
			trivial = true;
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			
			result = log.add(logEntry);
		}
		
		Stat stat = null;
		if (result != null) {
			if (result.toString().contains("Exception")) {
				ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
				exception.throwException();
			} else {
				if (trivial)
					stat = (Stat)result;
				else
					stat = (Stat)Serializer.deserializeStat((byte[]) result);
			}
		}
		
		if (logger.isTraceEnabled())
			logger.trace("EXISTS END");
		
		return stat;
	}


    @Override
    public void getChildren(final String path, Watcher watcher,
                            AsyncCallback.ChildrenCallback cb, Object ctx)
    {
        // TODO true asynchronous execution + correct return code
        try {
            cb.processResult(0,path,ctx,getChildren(path,watcher));
        } catch (KeeperException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        } catch (InterruptedException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }
    }

    @Override
	public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
		
		Command cmd = new CmdGetChildren(assignCmdId(), path, watcher);
		Object result = null;
		
		if (logger.isTraceEnabled())
			logger.trace("GETCHILDREN START");
		
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			
			result = log.add(logEntry);
		}
		
		List<String> children = new ArrayList<String>();
		if (result != null && result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		else {
			children = ((ArrayList<String>)result);
		}
		
		if (logger.isTraceEnabled())
			logger.trace("GETCHILDREN END");
		
		return children;
	}

    @Override
	public List<String> getChildren(String path, boolean watcher) throws KeeperException, InterruptedException {
		
		Command cmd = new CmdGetChildren(assignCmdId(), path, watcher);
		Object result = null;
		
		if (logger.isTraceEnabled())
			logger.trace("GETCHILDREN START");
		
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			
			result = log.add(logEntry);
		}
		
		List<String> children = new ArrayList<String>();
		if (result != null && result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		else {
			children = ((ArrayList<String>)result); // error:result is one string
		}
		
		if (logger.isTraceEnabled())
			logger.trace("GETCHILDREN END");
		
		return children;
	}

    @Override
	public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
		
		CmdGetData cmd = new CmdGetData(assignCmdId(), path, watcher, stat);
		Object result = null;
		
		if (logger.isTraceEnabled())
			logger.trace("GETDATA START");
		
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			
            result = log.add(logEntry);
		}
		
		if (result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		
		Data data = Serializer.deserializeData((byte[])result);
		
		if (stat != null) {
			Stat statResult = data.getStat();
			
			stat.setAversion(statResult.getAversion());
			stat.setCtime(statResult.getCtime());
			stat.setCversion(statResult.getCversion());
			stat.setCzxid(statResult.getCzxid());
			stat.setDataLength(statResult.getDataLength());
			stat.setEphemeralOwner(statResult.getEphemeralOwner());
			stat.setMtime(statResult.getMtime());
			stat.setMzxid(statResult.getMzxid());
			stat.setNumChildren(statResult.getNumChildren());
			stat.setPzxid(statResult.getPzxid());
			stat.setVersion(statResult.getVersion());
		}
		
		if (logger.isTraceEnabled())
			logger.trace("GETDATA END");
		
		return data.getData();
	}

    @Override
	public byte[] getData(String path, boolean watcher, Stat stat) throws KeeperException, InterruptedException {
		
		CmdGetData cmd = new CmdGetData(assignCmdId(), path, watcher, stat);
		Object result = null;
		boolean trivial = false;
		
		if (logger.isTraceEnabled())
			logger.trace("GETDATA START");
		
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
			trivial = true;
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			
			result = log.add(logEntry);

		}
		
		if (result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		
		if (trivial)
			return ((Data)result).getData();
		else {
			Data data = Serializer.deserializeData((byte[])result);
			
			if (stat != null) {
				Stat statResult = data.getStat();
				
				stat.setAversion(statResult.getAversion());
				stat.setCtime(statResult.getCtime());
				stat.setCversion(statResult.getCversion());
				stat.setCzxid(statResult.getCzxid());
				stat.setDataLength(statResult.getDataLength());
				stat.setEphemeralOwner(statResult.getEphemeralOwner());
				stat.setMtime(statResult.getMtime());
				stat.setMzxid(statResult.getMzxid());
				stat.setNumChildren(statResult.getNumChildren());
				stat.setPzxid(statResult.getPzxid());
				stat.setVersion(statResult.getVersion());
			}
			
			if (logger.isTraceEnabled())
				logger.trace("GETDATA END");
			
			return data.getData();
		}
	}

    @Override
	public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
		
		Command cmd = new CmdSetData(assignCmdId(), path, data, version);
		Object result = null;
		boolean trivial = false;
		
		if (logger.isTraceEnabled())
			logger.trace("SETDATA START");
		
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
			trivial = true;
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			
			result = log.add(logEntry);
		}
		
		if (result != null && result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		
		Stat stat = null;
		if (trivial)
			stat = (Stat) result;
		else
			stat = Serializer.deserializeStat((byte[])result);
		
		if (logger.isTraceEnabled())
			logger.trace("SETDATA END");
		
		return stat;
	}

    @Override
    public States getState(){
        return zookeepers.iterator().next().getState();
    }

    @Override
    public void close() throws InterruptedException {
        for(ZooKeeper zk : zookeepers){
            zk.close();
        }
    }

    @Override
    public void sync(final String path, AsyncCallback.VoidCallback cb, Object ctx){
        List<ZooKeeper> zks = new ArrayList<>(zookeepers);
         mappingFunc.computePartitions(path,zks);
        if(zks.size()!=1)
            throw new IllegalAccessError("invalid number of zks");
        zks.iterator().next().sync(path,cb,ctx);
    }

	public synchronized String assignCmdId() {
		String cmdId = String.valueOf(cmdNumber);
		cmdNumber++;
		
		if (logger.isTraceEnabled())
			logger.trace("assignCmdId " + cmdId);
		
		return cmdId;
	}
	
	public void setLeader(boolean leader) {
		this.leader = leader;
		
		if (logger.isTraceEnabled())
			logger.trace("setLeader");
	}
	
	private String assignClientId() {	
		String pid = String.valueOf(ManagementFactory.getRuntimeMXBean().getName().hashCode());
		
		if (logger.isTraceEnabled())
			logger.trace("assignClientId " + pid);
		
		return pid;
	}
}
