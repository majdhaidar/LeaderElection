package distributed.systems;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * The App class provides functionality to connect to a Zookeeper ensemble, manage leadership elections,
 * monitor target znodes, and handle changes to the state of the Zookeeper-connected environment.
 * This class implements the Watcher interface to react to Zookeeper events such as node creation,
 * deletion, data changes, and child changes.
 *
 * The application uses Zookeeper to participate in an election process and manage leadership responsibilities.
 * The target znode is also monitored for changes, and all relevant data is retrieved and displayed.
 */
public class App implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private static final String ELECTION_NAMESPACE = "/election";
    private String currentZnodeName;
    private static final String TARGET_ZNODE = "/target_znode";

    /**
     * The main entry point of the application, responsible for initializing the application,
     * connecting to Zookeeper, volunteering for leadership, electing a leader, watching
     * a target znode, running the main loop, and closing the connection.
     *
     * @param args command-line arguments passed to the program
     * @throws IOException if there is an error in connecting to Zookeeper
     * @throws InterruptedException if the thread is interrupted while waiting or performing operations
     * @throws KeeperException if there is an error while interacting with Zookeeper
     */
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

    /**
     * Makes the current instance a participant in the leader election process by creating a
     * Zookeeper ephemeral sequential znode in the election namespace.
     *
     * This method generates a znode with a unique, sequential name prefixed with the
     * election namespace and stores its name, which represents the participant's identity
     * in the election. The created znode ensures transient participation such that it is
     * removed when the session ends.
     *
     * @throws InterruptedException if the operation is interrupted.
     * @throws KeeperException if there is an error while interacting with Zookeeper.
     */
    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode created: " + znodeFullPath);
        currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    /**
     * Elects a leader from the nodes participating in the election process within the Zookeeper namespace.
     *
     * This method retrieves the list of child znodes in the election namespace, sorts them
     * lexicographically, and determines the smallest znode. The node associated with the
     * smallest znode is elected as the leader. If the current znode matches the smallest
     * znode, the current instance becomes the leader. Otherwise, the leader is identified
     * by the smallest znode.
     *
     * @throws InterruptedException if the operation is interrupted.
     * @throws KeeperException if there is an error while interacting with Zookeeper.
     */
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

    /**
     * Reevaluates leadership in the Zookeeper election process by monitoring and reacting to changes
     * in znodes within the election namespace.
     *
     * This method checks for the current instance's position in the list of znodes and determines
     * whether it is the leader. If it is not the leader, it sets a watch on the znode that is
     * immediately preceding the current instance's znode in the sorted order. If the predecessor
     * znode is deleted, leadership needs to be reevaluated.
     *
     * The method loops until a valid predecessor znode is found and a watch is successfully
     * established.
     *
     * @throws InterruptedException if the operation is interrupted.
     * @throws KeeperException if there is an error while interacting with Zookeeper.
     */
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

    /**
     * Establishes a new connection to a Zookeeper server using the specified address and session timeout.
     *
     * This method initializes a ZooKeeper instance with the provided ZOOKEEPER_ADDRESS and SESSION_TIMEOUT,
     * and registers the current instance as the event watcher to handle Zookeeper events.
     *
     * @throws IOException if an I/O error occurs while attempting to connect to the Zookeeper server.
     */
    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    /**
     * Executes the main processing loop of the application by synchronizing on the ZooKeeper instance.
     *
     * This method uses the `wait` mechanism on a synchronized block to pause execution
     * until notified. The ZooKeeper instance is used as the synchronization lock.
     *
     * @throws InterruptedException if the thread is interrupted while waiting.
     */
    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    /**
     * Closes the connection to the Zookeeper service.
     *
     * This method ensures that the ZooKeeper instance is properly closed,
     * releasing any resources held by the connection and unregistering
     * any watchers or listeners associated with it.
     *
     * @throws InterruptedException if the thread is interrupted during the closing process.
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    /**
     * Watches the target znode specified by the TARGET_ZNODE field in the Zookeeper instance.
     *
     * This method checks if the target znode exists in the Zookeeper namespace. If it exists,
     * it retrieves the data and the list of children associated with the znode, setting watches
     * for future changes on both. The retrieved information is then printed to the console.
     *
     * If the target znode does not exist, the method exits silently.
     *
     * This method sets watches on:
     * - Node existence.
     * - Data changes on the target znode.
     * - Changes to the target znode's children.
     *
     * Any Zookeeper-related exceptions are propagated to the caller.
     *
     * @throws KeeperException if an error occurs while interacting with the Zookeeper service.
     * @throws InterruptedException if the thread is interrupted during Zookeeper operations.
     */
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
