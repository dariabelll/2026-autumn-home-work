package company.vk.edu.distrib.compute.dariabelll.urlshortener;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.NoSuchElementException;
import java.util.Properties;

public class PropertiesDao implements Dao<String> {

    private final Path filePath;
    private final Properties properties;

    PropertiesDao(Path path) throws IOException {
        filePath = path.toAbsolutePath();
        properties = new Properties();

        Files.createDirectories(filePath.getParent());
        if (Files.notExists(filePath)) {
            Files.createFile(filePath);
        } else {
            try (Reader reader = Files.newBufferedReader(filePath)) {
                properties.load(reader);
            }
        }
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException {
        String value = properties.getProperty(key);
        if (value == null) {
            throw new NoSuchElementException(key);
        }
        return value;
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        String prevValue = properties.getProperty(key);
        properties.setProperty(key, value);
        try {
            save();
        } catch (IOException e) {
            if (prevValue == null) {
                properties.remove(key);
            } else {
                properties.setProperty(key, prevValue);
            }
            throw e;
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        String prevValue = properties.getProperty(key);
        if (prevValue == null) {
            return;
        }
        properties.remove(key);
        try {
            save();
        } catch (IOException e) {
            properties.setProperty(key, prevValue);
            throw e;
        }
    }

    @Override
    public void close() {
        // Files are opened and closed within each operation
    }

    private void save() throws IOException {
        Path tempFile = Files.createTempFile(
                filePath.getParent(),
                "properties-",
                ".tmp");
        try {
            try (Writer writer = Files.newBufferedWriter(tempFile)) {
                properties.store(writer, null);
            }
            Files.move(tempFile, filePath, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    public boolean isStorageAccessible() {
        Path directory = filePath.getParent();
        return directory != null
                && Files.isDirectory(directory)
                && Files.isWritable(directory)
                && Files.isExecutable(directory)
                && Files.isRegularFile(filePath)
                && Files.isReadable(filePath)
                && Files.isWritable(filePath);
    }
}
