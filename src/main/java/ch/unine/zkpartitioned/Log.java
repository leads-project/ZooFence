package ch.unine.zkpartitioned;

import org.apache.zookeeper.KeeperException;
import org.menagerie.DefaultZkSessionManager;
import org.menagerie.ZkSessionManager;
import org.menagerie.collections.ZkBlockingQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Log {


    private static final int LOG_ENTRY_UPDATE_TIMEOUT = 100;

    private ZkSessionManager manager;
    private Serializer serializer;
    private String logNode;			// name of the log node
    private ZkBlockingQueue<LogEntry> input; // concurrent queue holding input entries
    private ConcurrentHashMap<String, BlockingQueue<LogEntry>> output; // map of queues containing output entries

	/**
	 * Constructor
	 * @param key appended to /log
	 */
	public Log(String connectionString, int to, String key) {
		this.manager = new DefaultZkSessionManager(connectionString,to);
        serializer = new Serializer();
		logNode = "/log" + key;
        this.input = new ZkBlockingQueue<LogEntry>(logNode,serializer,this.manager);
        output = new ConcurrentHashMap<>();
    }
	
	public Object add(LogEntry logEntry) throws KeeperException, InterruptedException {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("[" + logNode + "] " + ZooKeeperPartitioned.clientId + " execute cmd " + logEntry.getCmd().getId());

        String clientId = logEntry.getClientId();
        if(!output.containsKey(clientId))
            this.output.put(
                    clientId,
                    new ZkBlockingQueue<LogEntry>(logNode+"/"+logEntry.getClientId(),serializer,this.manager));

        List<LogEntry> result = new ArrayList<LogEntry>();
        while(true){
            result.clear();
            input.put(logEntry);
            result.add(output.get(clientId).poll(LOG_ENTRY_UPDATE_TIMEOUT, TimeUnit.MILLISECONDS));
            output.get(clientId).drainTo(result);
            for(LogEntry e: result){
                if( e!=null && e.getCmd().equals(logEntry.getCmd()) )
                    return serializer.deserialize(e.getResult());
            }
        }

	}

	public LogEntry nextEntry() {
        while(true){
            try {
                LogEntry entry = input.take();
                return entry;
            } catch (InterruptedException e) {
                e.printStackTrace();  // TODO: Customise this generated block
            }
        }
	}
	
	public void updateEntry(LogEntry logEntry) throws KeeperException, InterruptedException {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("[" + logNode + "] " + ZooKeeperPartitioned.clientId + " recorded result for cmd " + logEntry.getCmd().getId());

        String clientId = logEntry.getClientId();
        if(!output.containsKey(clientId))
            this.output.put(
                    clientId,
                    new ZkBlockingQueue<LogEntry>(logNode+"/"+logEntry.getClientId(),serializer,this.manager));
        output.get(clientId).put(logEntry);
	}

}
