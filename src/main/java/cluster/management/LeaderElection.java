package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {

    // Variables Required to process ZooKeeper instance.
    private static final String ZOOKEEPER_ADRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private static final String ELECTION_NAMESPACE = "/election";
    private String currentZnodePath;
    private final OnElectionCallBack onElectionCallBack;

    public LeaderElection(ZooKeeper zooKeeper, OnElectionCallBack onElectionCallBack){
        this.zooKeeper = zooKeeper;
        this.onElectionCallBack = onElectionCallBack;
    }

    // Initiate ZooKeeper instance on main thread.
    /*public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZooKeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.reElectLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from ZooKeeper, exiting application");
    }*/

    public void volunteerForLeadership() throws KeeperException, InterruptedException{
        String znodePrefix  = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("znode name " + znodeFullPath);
        this.currentZnodePath = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reElectLeader() throws KeeperException, InterruptedException {
        Stat predecessorStat = null;
        String predecessorZnodeName = "";

        while(predecessorStat == null){
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

            Collections.sort(children);
            String smallestChild = children.get(0);
            if(currentZnodePath.equals(smallestChild)){
                System.out.println("I am the leader");
                onElectionCallBack.onEelectedToBeLeader();
                return;
            }else{
                System.out.println("I am not the leader");
                int predecessorIndex = Collections.binarySearch(children, currentZnodePath) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }
        onElectionCallBack.onWorker();
        System.out.println("Watching Node : " + predecessorZnodeName);
        System.out.println();
    }

    public void connectToZooKeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        // Keep main thread wait for events threads to process.
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
            case NodeDeleted:
                try {
                    reElectLeader();
                } catch (KeeperException e) {
                } catch (InterruptedException e) {
                }
        }
    }
}
