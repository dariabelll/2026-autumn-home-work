package company.vk.edu.distrib.compute.dariabelll.kv;

import company.vk.edu.distrib.compute.Dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class JournaledDao implements Dao<byte[]> {

    private static final Logger log = LoggerFactory.getLogger(JournaledDao.class);

    private static final byte PUT_OPERATION = 'P';
    private static final byte DELETE_OPERATION = 'D';

    private final Path filePath;
    private final Path directory;
    private final Map<String, byte[]> entries;
    private final RandomAccessFile journal;
    private final ReentrantLock writeLock = new ReentrantLock();
    private boolean journalFailed;

    public JournaledDao(Path path) throws IOException {
        filePath = path.toAbsolutePath();
        Path parent = filePath.getParent();
        if (parent == null) {
            throw new IllegalArgumentException("Journal path must have a parent directory: " + filePath);
        }
        directory = parent;
        entries = new ConcurrentHashMap<>();

        Files.createDirectories(directory);
        journal = new RandomAccessFile(filePath.toFile(), "rw");
        try {
            load();
        } catch (IOException | RuntimeException e) {
            try {
                journal.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        byte[] value = entries.get(key);
        if (value == null) {
            throw new NoSuchElementException(key);
        }
        return value.clone();
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        writeLock.lock();
        try {
            append(putRecord(key.getBytes(StandardCharsets.UTF_8), value));
            entries.put(key, value.clone());
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        writeLock.lock();
        try {
            append(deleteRecord(key.getBytes(StandardCharsets.UTF_8)));
            entries.remove(key);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (!journal.getChannel().isOpen()) {
                return;
            }
            try (journal) {
                if (journalFailed) {
                    throw new IOException("Cannot compact a journal after a failed rollback");
                }
                save();
            }
        } finally {
            writeLock.unlock();
        }
    }

    private static byte[] putRecord(byte[] key, byte[] value) throws IOException {
        ByteArrayOutputStream record = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(record)) {
            out.writeByte(PUT_OPERATION);
            out.writeInt(key.length);
            out.write(key);
            out.writeInt(value.length);
            out.write(value);
        }
        return record.toByteArray();
    }

    private static byte[] deleteRecord(byte[] key) throws IOException {
        ByteArrayOutputStream record = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(record)) {
            out.writeByte(DELETE_OPERATION);
            out.writeInt(key.length);
            out.write(key);
        }
        return record.toByteArray();
    }

    private void append(byte[] record) throws IOException {
        if (journalFailed) {
            throw new IOException("Journal rollback failed");
        }
        long start = journal.getFilePointer();
        try {
            journal.write(record);
        } catch (IOException e) {
            try {
                journal.setLength(start);
                journal.seek(start);
            } catch (IOException rollbackException) {
                journalFailed = true;
                e.addSuppressed(rollbackException);
            }
            throw e;
        }
    }

    private void load() throws IOException {
        while (journal.getFilePointer() < journal.length()) {
            long start = journal.getFilePointer();
            try {
                applyRecord();
            } catch (EOFException e) {
                log.warn("Journal {} has an incomplete record at offset {}, truncating {} bytes",
                        filePath, start, journal.length() - start);
                journal.setLength(start);
                journal.seek(start);
                break;
            }
        }
    }

    private void applyRecord() throws IOException {
        byte operation = journal.readByte();
        String key = new String(readField(), StandardCharsets.UTF_8);
        switch (operation) {
            case PUT_OPERATION -> entries.put(key, readField());
            case DELETE_OPERATION -> entries.remove(key);
            default -> throw new IOException(String.format(
                    "Journal %s: expected operation 'P' or 'D', got 0x%02X",
                    filePath, operation & 0xFF));
        }
    }

    private byte[] readField() throws IOException {
        int len = journal.readInt();
        if (len < 0) {
            throw new IOException(String.format(
                    "Journal %s: expected non-negative field length, got %d",
                    filePath, len));
        }
        if (len > journal.length() - journal.getFilePointer()) {
            throw new EOFException();
        }
        byte[] field = new byte[len];
        journal.readFully(field);
        return field;
    }

    private void save() throws IOException {
        Path tempFile = Files.createTempFile(
                directory,
                "journal-",
                ".tmp");
        try {
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(tempFile))) {
                for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                    out.write(putRecord(entry.getKey().getBytes(StandardCharsets.UTF_8), entry.getValue()));
                }
            }
            journal.close();
            Files.move(tempFile, filePath, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    public boolean isStorageAccessible() {
        writeLock.lock();
        try {
            return !journalFailed
                    && journal.getChannel().isOpen()
                    && Files.isDirectory(directory)
                    && Files.isWritable(directory)
                    && Files.isExecutable(directory)
                    && Files.isRegularFile(filePath)
                    && Files.isReadable(filePath)
                    && Files.isWritable(filePath);
        } finally {
            writeLock.unlock();
        }
    }
}
