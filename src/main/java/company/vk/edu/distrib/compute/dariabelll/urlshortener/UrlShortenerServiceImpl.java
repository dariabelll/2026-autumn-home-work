package company.vk.edu.distrib.compute.dariabelll.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;

public class UrlShortenerServiceImpl implements UrlShortenerService {

    private final HttpServer server;
    private final int port;
    private Dao<String> urlDao;
    private final JournaledDao userDao;

    private boolean configurationLocked;
    private @Nullable String kafkaBootstrapServers;
    private @Nullable UrlShortenerModeration moderation;

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
        if (configurationLocked) {
            throw new IllegalStateException("URL shortener can only be started once");
        }
        configurationLocked = true;
        try {
            String bootstrapServers = kafkaBootstrapServers;
            UrlShortenerModeration moderation = bootstrapServers == null
                    ? null : new UrlShortenerModeration(bootstrapServers, urlDao);
            this.moderation = moderation;
            server.createContext("/", new UrlShortenerHttpHandler(port, urlDao, userDao, moderation));
            if (moderation != null) {
                moderation.start();
            }
            server.start();
        } catch (RuntimeException | Error e) {
            try {
                stop();
            } catch (RuntimeException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
    }

    @Override
    public void stop() {
        configurationLocked = true;
        Dao<String> linksDao = urlDao;
        try (linksDao; userDao) {
            server.stop(1);
            UrlShortenerModeration moderation = this.moderation;
            if (moderation != null) {
                moderation.stop();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setKafkaBootstrapServers(String bootstrapServers) {
        if (configurationLocked) {
            throw new IllegalStateException("Kafka configuration can only be changed before start or stop");
        }
        kafkaBootstrapServers = bootstrapServers;
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (configurationLocked) {
            throw new IllegalStateException("Links DAO can be set only before start or stop");
        }
        try {
            urlDao.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        urlDao = dao;
    }
}
