package company.vk.edu.distrib.compute.dariabelll.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;

public class KVHttpHandler implements HttpHandler {

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;

    private static final String STATUS_ENDPOINT = "/v0/status";
    private static final String ENTITY_ENDPOINT = "/v0/entity";
    private static final String ID_QUERY_PARAMETER = "id=";

    private static final int EMPTY_RESPONSE_LENGTH = -1;

    private final JournaledDao dao;

    public KVHttpHandler(JournaledDao dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                handleRequest(exchange);
            } catch (NoSuchElementException e) {
                sendEmptyResponse(exchange, HTTP_NOT_FOUND);
            } catch (IOException e) {
                if (exchange.getResponseCode() != -1) {
                    throw e;
                }
                sendEmptyResponse(exchange, HTTP_INTERNAL_SERVER_ERROR);
            }
        }
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();

        List<String> allowedMethods = getAllowedMethods(path);
        if (allowedMethods.isEmpty()) {
            sendEmptyResponse(exchange, HTTP_NOT_FOUND);
            return;
        }
        if (!allowedMethods.contains(method)) {
            exchange.getResponseHeaders().set(
                    "Allow",
                    String.join(", ", allowedMethods)
            );
            sendEmptyResponse(exchange, HTTP_METHOD_NOT_ALLOWED);
            return;
        }
        if (STATUS_ENDPOINT.equals(path)) {
            handleGetStatus(exchange);
            return;
        }
        dispatchByMethod(exchange, method);
    }

    private void dispatchByMethod(
            HttpExchange exchange,
            String method
    ) throws IOException {
        String key = extractKey(exchange.getRequestURI());
        if (key == null) {
            sendEmptyResponse(exchange, HTTP_BAD_REQUEST);
            return;
        }
        switch (method) {
            case METHOD_GET -> handleGet(exchange, key);
            case METHOD_PUT -> handlePut(exchange, key);
            case METHOD_DELETE -> handleDelete(exchange, key);
            default -> throw new IllegalStateException("Unexpected HTTP method: " + method);
        }
    }

    private void handleGet(HttpExchange exchange, String key) throws IOException {
        byte[] value = dao.get(key);
        sendByteResponse(exchange, value);
    }

    private void handleGetStatus(HttpExchange exchange) throws IOException {
        int status = dao.isStorageAccessible()
                ? HTTP_OK
                : HTTP_SERVICE_UNAVAILABLE;
        sendEmptyResponse(exchange, status);
    }

    private void handlePut(HttpExchange exchange, String key) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        dao.upsert(key, value);
        sendEmptyResponse(exchange, HTTP_CREATED);
    }

    private void handleDelete(HttpExchange exchange, String key) throws IOException {
        dao.delete(key);
        sendEmptyResponse(exchange, HTTP_ACCEPTED);
    }

    private static @Nullable String extractKey(URI uri) {
        String query = uri.getRawQuery();
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            if (param.startsWith(ID_QUERY_PARAMETER)) {
                String key = URLDecoder.decode(param.substring(ID_QUERY_PARAMETER.length()), StandardCharsets.UTF_8);
                return key.isEmpty() ? null : key;
            }
        }
        return null;
    }

    private static List<String> getAllowedMethods(String path) {
        if (STATUS_ENDPOINT.equals(path)) {
            return List.of(METHOD_GET);
        }
        if (ENTITY_ENDPOINT.equals(path)) {
            return List.of(METHOD_GET, METHOD_PUT, METHOD_DELETE);
        }
        return List.of();
    }

    private static void sendEmptyResponse(
            HttpExchange exchange,
            int status
    ) throws IOException {
        exchange.sendResponseHeaders(status, EMPTY_RESPONSE_LENGTH);
    }

    private static void sendByteResponse(
            HttpExchange exchange,
            byte[] response
    ) throws IOException {
        if (response.length == 0) {
            sendEmptyResponse(exchange, HTTP_OK);
            return;
        }
        exchange.sendResponseHeaders(HTTP_OK, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }
}
