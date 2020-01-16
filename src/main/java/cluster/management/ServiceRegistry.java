package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.WatcherEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    private static final String REGESTRY_NODE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZnode = null;
    private List<String> allServiceAddresses = null;

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    public void registerToCluster(String metadata) throws KeeperException, InterruptedException{
        this.currentZnode = zooKeeper.create(REGESTRY_NODE + "/n_", metadata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to service registry");
    }

    private void createServiceRegistryZnode() {
        try {
            if(zooKeeper.exists(REGESTRY_NODE, false) == null){
                zooKeeper.create(REGESTRY_NODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
    }

    public void registerForUpdates() {
        try {
            updateAddress();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws KeeperException, InterruptedException {
        if(allServiceAddresses == null){
            updateAddress();
        }
        return allServiceAddresses;
    }

    public void unRegisterFromCluster() throws KeeperException, InterruptedException {
        if(currentZnode != null && zooKeeper.exists(currentZnode, false) != null){
            zooKeeper.delete(currentZnode, -1);
        }
    }


    private synchronized void updateAddress() throws KeeperException, InterruptedException {
        List<String> workerNodes = zooKeeper.getChildren(REGESTRY_NODE, this);
        List<String> addresses = new ArrayList<>(workerNodes.size());

        for(String workerNode : workerNodes){
            String workerZnodeFullPath = REGESTRY_NODE + "/" + workerNode;
            Stat stat = zooKeeper.exists(workerZnodeFullPath, false);
            if(stat == null){
                continue;
            }
            byte[] addressByte = zooKeeper.getData(workerZnodeFullPath, false, stat);
            String address = new String(addressByte);
            addresses.add(address);
        }
        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The cluster address are : " + this.allServiceAddresses);
    }

    @Override
    public void process(WatchedEvent event){
        switch(event.getType()){
            case NodeChildrenChanged:
                try {
                    updateAddress();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
        }
    }
}
