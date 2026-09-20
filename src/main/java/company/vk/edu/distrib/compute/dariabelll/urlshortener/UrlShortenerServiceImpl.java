package company.vk.edu.distrib.compute.dariabelll.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;

public class UrlShortenerServiceImpl implements UrlShortenerService {

    private final HttpServer server;
    private final int port;
    private final PropertiesDao urlDao;
    private final PropertiesDao userDao;

    public UrlShortenerServiceImpl(
            int port,
            PropertiesDao urlDao,
            PropertiesDao userDao) throws IOException {
        this.port = port;
        this.urlDao = urlDao;
        this.userDao = userDao;
        server = HttpServer.create();
        server.createContext(
                "/",
                new UrlShortenerHttpHandler(port, urlDao, userDao)
        );
    }

    @Override
    public void start() {
        try {
            server.bind(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
        urlDao.close();
        userDao.close();
    }
}
