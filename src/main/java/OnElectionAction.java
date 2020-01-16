import cluster.management.OnElectionCallBack;
import cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class OnElectionAction implements OnElectionCallBack {

    private final ServiceRegistry serviceRegistry;
    private final int port;

    public OnElectionAction(ServiceRegistry serviceRegistry, int port){
        this.serviceRegistry = serviceRegistry;
        this.port = port;
    }

    @Override
    public void onEelectedToBeLeader() {
        try {
            serviceRegistry.unRegisterFromCluster();
            serviceRegistry.registerForUpdates();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onWorker() {
        try {
            String currentServerAddress = String.format("http://%s:%d", InetAddress.getLocalHost().getCanonicalHostName(), port);
            serviceRegistry.registerToCluster(currentServerAddress);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
