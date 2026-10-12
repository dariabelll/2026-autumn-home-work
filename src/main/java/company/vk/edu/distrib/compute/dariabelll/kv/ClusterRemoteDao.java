package company.vk.edu.distrib.compute.dariabelll.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.NoSuchElementException;

public class ClusterRemoteDao implements Dao<String> {

    private final int[] ports;
    private final RemoteDao[] remoteDaos;

    public ClusterRemoteDao(int... ports) {
        this.ports = ports.clone();
        this.remoteDaos = new RemoteDao[this.ports.length];
        for (int i = 0; i < this.ports.length; ++i) {
            remoteDaos[i] = new RemoteDao(this.ports[i]);
        }
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return remoteDaos[selectPortIndex(key)].get(key);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        remoteDaos[selectPortIndex(key)].upsert(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        remoteDaos[selectPortIndex(key)].delete(key);
    }

    @Override
    public void close() throws IOException {
        for (RemoteDao remoteDao : remoteDaos) {
            remoteDao.close();
        }
    }

    private int selectPortIndex(String key) {
        int bestPortIndex = 0;
        long maxWeight = computeHash(key, ports[0]);

        for (int i = 1; i < this.ports.length; ++i) {
            long weight = computeHash(key, ports[i]);
            if (weight > maxWeight) {
                maxWeight = weight;
                bestPortIndex = i;
            }
        }

        return bestPortIndex;
    }

    private static long computeHash(String key, int port) {
        String input = key + "|" + port;
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(bytes);

            return ByteBuffer.wrap(hashBytes).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
