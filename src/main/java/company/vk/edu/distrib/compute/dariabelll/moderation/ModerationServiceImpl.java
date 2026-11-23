package company.vk.edu.distrib.compute.dariabelll.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class ModerationServiceImpl implements ModerationService {

    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(3);
    private static final long SEND_TIMEOUT_SECONDS = 3;

    private final ReentrantLock startStopLock = new ReentrantLock();
    private final String bootstrapServers;
    private @Nullable KafkaProducer<String, String> producer;
    private @Nullable KafkaConsumer<String, String> consumer;
    private @Nullable Thread consumerThread;
    private boolean startAllowed = true;
    private final AtomicBoolean running = new AtomicBoolean();
    private @Nullable RuntimeException workerFailure;
    private final List<String> bannedWords;

    public ModerationServiceImpl(String bootstrapServers) throws IOException {
        this.bootstrapServers = bootstrapServers;

        bannedWords = ModerationUtils.loadBannedWords();
    }

    @Override
    public void start() {
        startStopLock.lock();
        try {
            if (!startAllowed) {
                throw new IllegalStateException("Moderation service can only be started once");
            }
            startAllowed = false;
            try {
                KafkaProducer<String, String> producer = ModerationUtils.createProducer(bootstrapServers);
                this.producer = producer;
                KafkaConsumer<String, String> consumer = ModerationUtils.createConsumer(
                        bootstrapServers, "dariabelll-moderation"
                );
                this.consumer = consumer;
                consumer.subscribe(List.of("links.created"));
                running.set(true);
                Thread thread = new Thread(
                        () -> processLinks(consumer, producer), "moderation-consumer"
                );
                thread.start();
                consumerThread = thread;
            } catch (RuntimeException | Error e) {
                running.set(false);
                try {
                    closeClients();
                } catch (RuntimeException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                throw e;
            }
        } finally {
            startStopLock.unlock();
        }
    }

    @Override
    public void stop() {
        startStopLock.lock();
        try {
            startAllowed = false;
            Thread thread = consumerThread;
            if (thread == null) {
                return;
            }
            running.set(false);
            KafkaConsumer<String, String> consumer = this.consumer;
            if (thread.isAlive() && consumer != null) {
                consumer.wakeup();
            }
            awaitTermination(thread);
            RuntimeException failure = workerFailure;
            if (failure != null) {
                throw new IllegalStateException("Moderation worker failed", failure);
            }
        } finally {
            startStopLock.unlock();
        }
    }

    private static void awaitTermination(Thread thread) {
        try {
            thread.join(STOP_TIMEOUT.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping moderation", e);
        }
        if (thread.isAlive()) {
            throw new IllegalStateException("Moderation worker did not stop within " + STOP_TIMEOUT);
        }
    }

    private void processLinks(
            KafkaConsumer<String, String> consumer,
            KafkaProducer<String, String> producer) {
        try {
            consumeLinks(consumer, producer);
        } catch (WakeupException e) {
            if (running.get()) {
                workerFailure = e;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workerFailure = new IllegalStateException("Interrupted while publishing moderation response", e);
        } catch (ExecutionException | TimeoutException e) {
            workerFailure = new IllegalStateException("Failed to publish moderation response", e);
        } catch (RuntimeException e) {
            workerFailure = e;
        } finally {
            finishProcessing();
        }
    }

    private void consumeLinks(
            KafkaConsumer<String, String> consumer,
            KafkaProducer<String, String> producer) throws InterruptedException, ExecutionException, TimeoutException {
        while (running.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                if (!running.get()) {
                    break;
                }
                producer.send(createResponse(record)).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                ModerationUtils.commitRecord(consumer, record, COMMIT_TIMEOUT);
            }
        }
    }

    private ProducerRecord<String, String> createResponse(ConsumerRecord<String, String> record) {
        try {
            LinkCreatedEvent event = ModerationUtils.parseLinkCreatedEvent(record.key(), record.value());
            String url = URLDecoder.decode(event.url(), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
            String reason = bannedWords.stream().filter(url::contains).findFirst().orElse("");
            ModerationResultEvent result = new ModerationResultEvent(
                    event.id(), reason.isEmpty(), reason, Instant.now()
            );
            return new ProducerRecord<>(
                    "links.moderated", event.id(), ModerationUtils.formatModerationResult(result)
            );
        } catch (IllegalArgumentException e) {
            ProducerRecord<String, String> deadLetter = new ProducerRecord<>(
                    "links.created.dlt", null, record.key(), record.value(), record.headers()
            );
            deadLetter.headers().add("error", e.toString().getBytes(StandardCharsets.UTF_8));
            return deadLetter;
        }
    }

    private void finishProcessing() {
        running.set(false);
        try {
            closeClients();
        } catch (RuntimeException closeFailure) {
            if (workerFailure == null) {
                workerFailure = closeFailure;
            } else {
                workerFailure.addSuppressed(closeFailure);
            }
        }
    }

    private void closeClients() {
        RuntimeException failure = null;
        try {
            if (consumer != null) {
                consumer.close(CloseOptions.timeout(CLOSE_TIMEOUT));
            }
        } catch (RuntimeException e) {
            failure = e;
        }
        try {
            if (producer != null) {
                producer.close(CLOSE_TIMEOUT);
            }
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

}
