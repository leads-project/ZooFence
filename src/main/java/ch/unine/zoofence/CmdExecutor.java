package ch.unine.zoofence;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class CmdExecutor extends Thread {
    private Command cmd;
    private ZooKeeper zk;
    private List<Object> results;

    public CmdExecutor(Command cmd, ZooKeeper zk, List<Object> results) {
        this.cmd = cmd;
        this.zk = zk;
        this.results = results;
    }

    @Override
    public void run() {
        if (ZooFence.logger.isTraceEnabled())
            ZooFence.logger.trace("CmdExecutor.run");

        Object result = null;

        try {
            result = cmd.execute(zk);
        } catch (KeeperException | InterruptedException | IllegalArgumentException e) {
            result = e;
        }
        finally {

            synchronized (results) {
                results.add(result);
                results.notifyAll();
            }
        }
    }
}
