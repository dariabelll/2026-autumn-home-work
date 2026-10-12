package company.vk.edu.distrib.compute.dariabelll.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

@ClusterDaoFactoryTest
public class ClusterRemoteDaoFactoryImpl implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("At least one KV service port is required");
        }
        return new ClusterRemoteDao(ports);
    }
}
