package company.vk.edu.distrib.compute.dariabelll.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;

public class RemoteDao implements Dao<String> {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(1);

    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_NOT_FOUND = 404;

    private final HttpClient client;
    private final String entityUrl;

    public RemoteDao(int port) {
        this.client = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .build();
        this.entityUrl = "http://localhost:" + port + "/v0/entity?id=";
    }

    @Override
    public String get(String key) throws NoSuchElementException, IOException {
        URI uri = URI.create(entityUrl + URLEncoder.encode(key, StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest
                .newBuilder(uri)
                .timeout(REQUEST_TIMEOUT)
                .GET().build();
        HttpResponse<byte[]> response = send(request, HttpResponse.BodyHandlers.ofByteArray());
        return switch (response.statusCode()) {
            case HTTP_OK -> new String(response.body(), StandardCharsets.UTF_8);
            case HTTP_NOT_FOUND -> throw new NoSuchElementException(key);
            default -> throw new IOException("GET " + key + ": unexpected status " + response.statusCode());
        };
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(
                entityUrl + URLEncoder.encode(key, StandardCharsets.UTF_8))
                )
                .timeout(REQUEST_TIMEOUT)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(value.getBytes(StandardCharsets.UTF_8)))
                .build();
        int status = send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        if (status != HTTP_CREATED) {
            throw new IOException("PUT " + key + ": unexpected status " + status);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(
                entityUrl + URLEncoder.encode(key, StandardCharsets.UTF_8))
                )
                .timeout(REQUEST_TIMEOUT)
                .DELETE()
                .build();
        int status = send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        if (status != HTTP_ACCEPTED) {
            throw new IOException("DELETE " + key + ": unexpected status " + status);
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler) throws IOException {
        try {
            return client.send(request, handler);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for KV response", e);
        }
    }
}
