package distributed.systems;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Hello world!
 */
public class App implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private static final String ELECTION_NAMESPACE = "/election";
    private String currentZnodeName;
    private static final String TARGET_ZNODE = "/target_znode";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        App app = new App();
        app.connectToZookeeper();
        app.volunteerForLeadership();
        app.electLeader();
        app.watchTargetZnode();
        app.run();
        app.close();
        System.out.println("Disconnected from Zookeeper");
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode created: " + znodeFullPath);
        currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void electLeader() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);
        String smallestChild = children.get(0);

        if (smallestChild.equals(currentZnodeName)) {
            System.out.println("I am the leader of " + currentZnodeName);
            return;
        }
        System.out.println("I am the leader of " + smallestChild);
    }

    public void reelectLeader() throws InterruptedException, KeeperException {
        String predecessorZnodeName = "";
        Stat predeccesorStat = null;
        while (predeccesorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader of " + currentZnodeName);
            }
            else {
                System.out.println("I am not the leader");
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predeccesorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }
        System.out.println("Watching znode " + predecessorZnodeName);
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void watchTargetZnode() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
        if (null == stat) {
            return;
        }
        byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("Date : " + new String(data) + " Children : " + children);
    }


    /**
     * Will be called by the zookeeper library in a separate event
     *
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Connected to Zookeeper");
                }
                else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notify();
                    }
                }
                break;

            case NodeDeleted:
                System.out.println("Zookeeper Node Deleted " + TARGET_ZNODE);
                try {
                    reelectLeader();
                }
                catch (InterruptedException e) {
                }
                catch (KeeperException e) {
                }
                break;
            case NodeCreated:
                System.out.println("Zookeeper Node Created " + TARGET_ZNODE);
                break;
            case NodeDataChanged:
                System.out.println("Zookeeper Node DataChanged " + TARGET_ZNODE);
                break;
            case NodeChildrenChanged:
                System.out.println("Zookeeper Node ChildrenChanged " + TARGET_ZNODE);
                break;
        }

        try {
            // get all the upto-date data and print to screen
            watchTargetZnode();
        }
        catch (KeeperException e) {
        }
        catch (InterruptedException e) {
        }
    }
}
