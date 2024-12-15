package distributed.systems;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private static final String ELECTION_NAMESPACE = "/election";
    private String currentZnodeName;

    public static void main( String[] args ) throws IOException, InterruptedException, KeeperException {
        App app = new App();
        app.connectToZookeeper();
        app.volunteerForLeadership();
        app.electLeader();
        app.run();
        app.close();
        System.out.println("Disconnected from Zookeeper");
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE+"/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode created: " + znodeFullPath);
        currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE+"/", "");
    }

    public void electLeader() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);
        String smallestChild = children.get(0);
        if(smallestChild.equals(currentZnodeName)) {
            System.out.println("I am the leader of " + currentZnodeName);
            return;
        }
        System.out.println("I am not the leader, "+smallestChild+" is the leader of "+currentZnodeName);
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

    /**
     * Will be called by the zookeeper library in a separate event
     *
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()){
            case None:
                if(event.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("Connected to Zookeeper");
                }else {
                    synchronized (zooKeeper){
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notify();
                    }
                }
                break;
        }
    }
}
