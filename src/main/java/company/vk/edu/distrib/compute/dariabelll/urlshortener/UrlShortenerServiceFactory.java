package company.vk.edu.distrib.compute.dariabelll.urlshortener;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class UrlShortenerServiceFactory extends AbstractHttpServiceFactory<UrlShortenerServiceImpl> {

    private static final Path DATA_DIRECTORY = Path.of(
            System.getProperty("java.io.tmpdir"),
            "dariabelll-url-shortener"
    );
    private static final Path URL_FILE_PATH = DATA_DIRECTORY.resolve("links.properties");
    private static final Path USER_FILE_PATH = DATA_DIRECTORY.resolve("users.properties");

    @Override
    protected UrlShortenerServiceImpl doCreate(int port) throws IOException {
        PropertiesDao urlDao = new PropertiesDao(URL_FILE_PATH);
        PropertiesDao userDao = new PropertiesDao(USER_FILE_PATH);
        return new UrlShortenerServiceImpl(port, urlDao, userDao);
    }
}
