package company.vk.edu.distrib.compute.dariabelll.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVServiceTest;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@KVServiceTest
public class KVServiceFactory extends AbstractHttpServiceFactory<KVServiceImpl> {

    private static final Path DATA_DIRECTORY = Path.of(
            System.getProperty("java.io.tmpdir"),
            "dariabelll-kv"
    );
    private static final String FILE_NAME = "journal.bin";

    @Override
    protected KVServiceImpl doCreate(int port) throws IOException {
        Path filePath = DATA_DIRECTORY.resolve(String.valueOf(port)).resolve(FILE_NAME);
        JournaledDao dao = new JournaledDao(filePath);
        try {
            return new KVServiceImpl(port, dao, createExecutor());
        } catch (IOException | RuntimeException e) {
            closeOnFailure(dao, e);
            throw e;
        }
    }

    private static @Nullable ExecutorService createExecutor() {
        String threads = System.getenv("KV_THREADS");
        if (threads == null || threads.isBlank()) {
            return null;
        }
        return Executors.newFixedThreadPool(Integer.parseInt(threads));
    }

    private static void closeOnFailure(JournaledDao dao, Exception failure) {
        try {
            dao.close();
        } catch (IOException e) {
            failure.addSuppressed(e);
        }
    }
}
