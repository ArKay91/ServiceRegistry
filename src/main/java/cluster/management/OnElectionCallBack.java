package cluster.management;

public interface OnElectionCallBack {

    void onEelectedToBeLeader();

    void onWorker();
}
