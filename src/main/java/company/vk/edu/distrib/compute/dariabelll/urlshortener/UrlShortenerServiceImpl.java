package company.vk.edu.distrib.compute.dariabelll.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;

public class UrlShortenerServiceImpl implements UrlShortenerService {

    private final HttpServer server;
    private final int port;
    private Dao<String> urlDao;
    private final JournaledDao userDao;

    private boolean started;

    public UrlShortenerServiceImpl(
            int port,
            JournaledDao urlDao,
            JournaledDao userDao) throws IOException {
        this.port = port;
        this.urlDao = urlDao;
        this.userDao = userDao;
        server = HttpServer.create(new InetSocketAddress(port), 0);
    }

    @Override
    public void start() {
        server.createContext(
                "/",
                new UrlShortenerHttpHandler(port, urlDao, userDao)
        );
        server.start();
        started = true;
    }

    @Override
    public void stop() {
        Dao<String> locUrlsDao = urlDao;
        try (locUrlsDao; userDao) {
            server.stop(1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (started) {
            throw new IllegalStateException("Links DAO can be set only before start");
        }
        try {
            urlDao.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        urlDao = dao;
    }
}
