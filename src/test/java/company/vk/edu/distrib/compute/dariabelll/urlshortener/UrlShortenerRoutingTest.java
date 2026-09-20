package company.vk.edu.distrib.compute.dariabelll.urlshortener;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UrlShortenerRoutingTest {

    @TempDir
    Path directory;

    @Test
    void extractRedirectIdPreservesInvalidIds() {
        assertEquals("Abc1234567", UrlShortenerHttpUtils.extractRedirectId("/Abc1234567"));
        assertEquals("invalid-id", UrlShortenerHttpUtils.extractRedirectId("/invalid-id"));
        assertEquals("", UrlShortenerHttpUtils.extractRedirectId("/"));
        assertNull(UrlShortenerHttpUtils.extractRedirectId("/unknown/path"));
        assertNull(UrlShortenerHttpUtils.extractRedirectId("/Abc1234567/"));
        assertNull(UrlShortenerHttpUtils.extractRedirectId("Abc1234567"));
    }

    @Test
    void validateServerAddress() {
        for (String link : new String[]{
                "http://example.com", "https://localhost:65535/path?q=1#part",
                "http://[::1]:8080/", "https://example.com/path%20name"
        }) {
            assertFalse(RequestValidators.isInvalidLink(link), link);
        }
        for (String link : new String[]{
                "ftp://example.com", "https:///path", "https://example.com:65536",
                "http://example.com:-1", "http://example.com:abc", "http://bad_host/",
                "http://[invalid]/", "relative/path", "https://example.com/a b"
        }) {
            assertTrue(RequestValidators.isInvalidLink(link), link);
        }
    }

    @Test
    void routeMethodsAndInvalidPaths() throws Exception {
        try (PropertiesDao linkDao = new PropertiesDao(directory.resolve("links.properties"));
             PropertiesDao userDao = new PropertiesDao(directory.resolve("users.properties"));
             HttpClient httpClient = HttpClient.newHttpClient()) {
            userDao.upsert("user", "pass");
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            int port = server.getAddress().getPort();
            server.createContext("/", new UrlShortenerHttpHandler(port, linkDao, userDao));
            server.start();
            try {
                assertResponse(httpClient, port, "POST", "/v0/status", false, 405, "GET");
                assertResponse(httpClient, port, "GET", "/internal/users", false, 405, "POST");
                assertResponse(httpClient, port, "GET", "/v0/links", true, 405, "POST");
                assertResponse(httpClient, port, "PATCH", "/v0/links/Abc1234567", true,
                        405, "GET, PUT, DELETE");
                assertResponse(httpClient, port, "POST", "/Abc1234567", true, 405, "GET");
                assertResponse(httpClient, port, "PATCH", "/unknown/path", true, 404, "");
                assertResponse(httpClient, port, "GET", "/invalid-id", false, 422, "");
                assertResponse(httpClient, port, "GET", "/", false, 422, "");
                assertResponse(httpClient, port, "GET", "/Abc1234567", false, 404, "");
                assertResponse(httpClient, port, "GET", "/v0/links/invalid-id", true, 422, "");
                assertResponse(httpClient, port, "GET", "/v0/links/Abc1234567", false, 401, "");
            } finally {
                server.stop(0);
            }
        }
    }

    private static void assertResponse(HttpClient httpClient, int port, String method, String path,
                                       boolean authenticated, int status, String allow) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(5))
                .method(method, HttpRequest.BodyPublishers.noBody());
        if (authenticated) {
            request.header("Authorization", "Basic dXNlcjpwYXNz");
        }
        HttpResponse<Void> response = httpClient.send(request.build(), HttpResponse.BodyHandlers.discarding());
        assertEquals(status, response.statusCode(), method + " " + path);
        assertEquals(allow, response.headers().firstValue("Allow").orElse(""), method + " " + path);
    }
}
