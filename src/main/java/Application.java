import cluster.management.LeaderElection;
import cluster.management.OnElectionCallBack;
import cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 8080;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        Application application = new Application();
        ZooKeeper zooKeeper = application.connectToZooKeeper();

        ServiceRegistry serviceRegistry = new ServiceRegistry(zooKeeper);
        OnElectionCallBack onElectionAction = new OnElectionAction(serviceRegistry, currentServerPort);


        LeaderElection leaderElection = new LeaderElection(zooKeeper, onElectionAction);
        leaderElection.volunteerForLeadership();
        leaderElection.reElectLeader();

        application.run();
        application.close();
        System.out.println("Disconnected from ZooKeeper, exiting application");
    }

    public ZooKeeper connectToZooKeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zooKeeper;
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent event){
        switch(event.getType()) {
            case None:
                if(event.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("Successfully connected to ZooKeeper");
                }
        }
    }
}
