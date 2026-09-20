package company.vk.edu.distrib.compute.dariabelll.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

import static company.vk.edu.distrib.compute.dariabelll.urlshortener.UrlShortenerHttpUtils.extractRedirectId;
import static company.vk.edu.distrib.compute.dariabelll.urlshortener.UrlShortenerHttpUtils.readRequestBody;
import static company.vk.edu.distrib.compute.dariabelll.urlshortener.UrlShortenerHttpUtils.sendEmptyResponse;
import static company.vk.edu.distrib.compute.dariabelll.urlshortener.UrlShortenerHttpUtils.sendTextResponse;

public class UrlShortenerHttpHandler implements HttpHandler {

    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_MOVED_PERMANENTLY = 301;
    private static final int HTTP_UNAUTHORIZED = 401;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_UNPROCESSABLE_CONTENT = 422;
    private static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;

    private static final String STATUS_ENDPOINT = "/v0/status";
    private static final String USERS_ENDPOINT = "/internal/users";
    private static final String LINKS_ENDPOINT = "/v0/links";
    private static final String LINK_BY_ID_PREFIX = LINKS_ENDPOINT + "/";

    private static final String AUTHENTICATION_CHALLENGE =
            "Basic realm=\"url-shortener\", charset=\"UTF-8\"";
    private static final String ALPHA_NUMERIC_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private final int port;
    private final PropertiesDao urlDao;
    private final PropertiesDao userDao;
    private final UrlShortenerAuthentication authentication;

    public UrlShortenerHttpHandler(
            int port,
            PropertiesDao urlDao,
            PropertiesDao userDao) {
        this.port = port;
        this.urlDao = urlDao;
        this.userDao = userDao;
        authentication = new UrlShortenerAuthentication(userDao);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                String method = exchange.getRequestMethod();
                String path = exchange.getRequestURI().getPath();
                if (!isPublicEndpoint(method, path)
                        && authentication.isUnauthenticated(exchange)
                ) {
                    exchange.getResponseHeaders().set(
                            "WWW-Authenticate",
                            AUTHENTICATION_CHALLENGE
                    );
                    sendEmptyResponse(exchange, HTTP_UNAUTHORIZED);
                    return;
                }
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
                switch (method) {
                    case "GET" -> handleGet(exchange, path);
                    case "POST" -> handlePost(exchange, path);
                    case "PUT" -> handlePut(exchange, path);
                    case "DELETE" -> handleDelete(exchange, path);
                    default -> throw new IllegalStateException("Unexpected HTTP method: " + method);
                }
            } catch (IOException e) {
                if (exchange.getResponseCode() != -1) {
                    throw e;
                }
                sendEmptyResponse(exchange, HTTP_INTERNAL_SERVER_ERROR);
            }
        }
    }

    private void handleGet(HttpExchange exchange, String path) throws IOException {
        if (STATUS_ENDPOINT.equals(path)) {
            handleGetStatus(exchange);
            return;
        }

        if (path.startsWith(LINK_BY_ID_PREFIX)) {
            String id = path.substring(LINK_BY_ID_PREFIX.length());
            handleGetLink(exchange, id);
            return;
        }

        String id = extractRedirectId(path);
        if (id != null) {
            handleRedirect(exchange, id);
            return;
        }

        sendEmptyResponse(exchange, HTTP_NOT_FOUND);
    }

    private void handleGetStatus(HttpExchange exchange) throws IOException {
        int status = urlDao.isStorageAccessible() && userDao.isStorageAccessible()
                ? HTTP_OK
                : HTTP_SERVICE_UNAVAILABLE;
        sendEmptyResponse(exchange, status);
    }

    private void handleGetLink(HttpExchange exchange, String id) throws IOException {
        if (RequestValidators.isInvalidId(id)) {
            sendEmptyResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
            return;
        }

        try {
            String longLink = urlDao.get(id);
            sendTextResponse(exchange, HTTP_OK, longLink);
        } catch (NoSuchElementException e) {
            sendEmptyResponse(exchange, HTTP_NOT_FOUND);
        }
    }

    private void handleRedirect(HttpExchange exchange, String id) throws IOException {
        if (RequestValidators.isInvalidId(id)) {
            sendEmptyResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
            return;
        }

        try {
            String longLink = urlDao.get(id);
            exchange.getResponseHeaders().set("Location", longLink);
            sendEmptyResponse(exchange, HTTP_MOVED_PERMANENTLY);
        } catch (NoSuchElementException e) {
            sendEmptyResponse(exchange, HTTP_NOT_FOUND);
        }
    }

    private void handlePost(HttpExchange exchange, String path) throws IOException {
        if (LINKS_ENDPOINT.equals(path)) {
            handlePostLink(exchange);
            return;
        }
        if (USERS_ENDPOINT.equals(path)) {
            handlePostUser(exchange);
            return;
        }

        sendEmptyResponse(exchange, HTTP_NOT_FOUND);
    }

    private void handlePostLink(HttpExchange exchange) throws IOException {
        String longLink = readRequestBody(exchange);

        if (RequestValidators.isInvalidLink(longLink)) {
            sendEmptyResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
            return;
        }

        String id = generateUniqueId();
        urlDao.upsert(id, longLink);
        String shortLink = "http://localhost:" + port + "/" + id;
        sendTextResponse(exchange, HTTP_CREATED, shortLink);
    }

    private void handlePostUser(HttpExchange exchange) throws IOException {
        String[] registryBody = readRequestBody(exchange).split(":", 2);
        if (RequestValidators.isInvalidRegistryBody(registryBody)) {
            sendEmptyResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
            return;
        }

        String nickname = registryBody[0];
        String password = registryBody[1];
        userDao.upsert(nickname, password);
        sendEmptyResponse(exchange, HTTP_OK);
    }

    private void handlePut(HttpExchange exchange, String path) throws IOException {
        if (!path.startsWith(LINK_BY_ID_PREFIX)) {
            sendEmptyResponse(exchange, HTTP_NOT_FOUND);
            return;
        }

        String id = path.substring(LINK_BY_ID_PREFIX.length());
        if (RequestValidators.isInvalidId(id)) {
            sendEmptyResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
            return;
        }

        String longLink = readRequestBody(exchange);

        if (RequestValidators.isInvalidLink(longLink)) {
            sendEmptyResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
            return;
        }
        try {
            urlDao.get(id);
        } catch (NoSuchElementException e) {
            sendEmptyResponse(exchange, HTTP_NOT_FOUND);
            return;
        }

        urlDao.upsert(id, longLink);
        sendEmptyResponse(exchange, HTTP_OK);
    }

    private void handleDelete(HttpExchange exchange, String path) throws IOException {
        if (!path.startsWith(LINK_BY_ID_PREFIX)) {
            sendEmptyResponse(exchange, HTTP_NOT_FOUND);
            return;
        }

        String id = path.substring(LINK_BY_ID_PREFIX.length());
        if (RequestValidators.isInvalidId(id)) {
            sendEmptyResponse(exchange, HTTP_UNPROCESSABLE_CONTENT);
            return;
        }

        urlDao.delete(id);
        sendEmptyResponse(exchange, HTTP_ACCEPTED);
    }

    private static List<String> getAllowedMethods(String path) {
        if (STATUS_ENDPOINT.equals(path)) {
            return List.of("GET");
        }
        if (USERS_ENDPOINT.equals(path) || LINKS_ENDPOINT.equals(path)) {
            return List.of("POST");
        }
        if (path.startsWith(LINK_BY_ID_PREFIX)) {
            return List.of("GET", "PUT", "DELETE");
        }
        if (extractRedirectId(path) != null) {
            return List.of("GET");
        }
        return List.of();
    }

    private static boolean isPublicEndpoint(String method, String path) {
        if (STATUS_ENDPOINT.equals(path) || USERS_ENDPOINT.equals(path)) {
            return true;
        }
        return "GET".equals(method) && extractRedirectId(path) != null;
    }

    private String generateUniqueId() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (true) {
            StringBuilder idBuilder = new StringBuilder(RequestValidators.ID_SIZE);
            for (int i = 0; i < RequestValidators.ID_SIZE; ++i) {
                int charIndex = random.nextInt(ALPHA_NUMERIC_ALPHABET.length());
                idBuilder.append(ALPHA_NUMERIC_ALPHABET.charAt(charIndex));
            }
            String id = idBuilder.toString();
            try {
                urlDao.get(id);
            } catch (NoSuchElementException e) {
                return id;
            }
        }
    }
}
