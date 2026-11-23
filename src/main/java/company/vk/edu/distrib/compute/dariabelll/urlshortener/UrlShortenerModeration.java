package company.vk.edu.distrib.compute.dariabelll.urlshortener;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.dariabelll.moderation.ModerationUtils;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

final class UrlShortenerModeration {

    private static final Duration API_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final long RETRY_DELAY_MILLIS = 500;

    private final String bootstrapServers;
    private final Dao<String> urlDao;
    private final BlockingQueue<LinkCreatedEvent> linkEvents = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final Thread producerThread = new Thread(this::sendEvents, "shortener-moderation-producer");
    private final Thread consumerThread = new Thread(this::readResults, "shortener-moderation-consumer");

    UrlShortenerModeration(String bootstrapServers, Dao<String> urlDao) {
        this.bootstrapServers = bootstrapServers;
        this.urlDao = urlDao;
    }

    void start() {
        running.set(true);
        producerThread.start();
        consumerThread.start();
    }

    void enqueue(LinkCreatedEvent event) {
        if (running.get()) {
            linkEvents.add(event);
        }
    }

    void stop() {
        running.set(false);
        producerThread.interrupt();
        consumerThread.interrupt();
        awaitTermination(producerThread);
        awaitTermination(consumerThread);
        linkEvents.clear();
    }

    private static void awaitTermination(Thread thread) {
        try {
            thread.join(STOP_TIMEOUT.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping moderation client", e);
        }
        if (thread.isAlive()) {
            throw new IllegalStateException("Moderation client thread did not stop: " + thread.getName());
        }
    }

    private void sendEvents() {
        try {
            LinkCreatedEvent pendingEvent = linkEvents.take();
            while (running.get()) {
                try {
                    KafkaProducer<String, String> producer = ModerationUtils.createProducer(bootstrapServers);
                    try {
                        while (running.get()) {
                            producer.send(createLinkMessage(pendingEvent)).get();
                            pendingEvent = linkEvents.take();
                        }
                    } finally {
                        closeProducer(producer);
                    }
                } catch (ExecutionException | RuntimeException e) {
                    pauseAfterFailure();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ProducerRecord<String, String> createLinkMessage(LinkCreatedEvent event) {
        return new ProducerRecord<>("links.created", event.id(), ModerationUtils.formatLinkCreatedEvent(event));
    }

    private void readResults() {
        while (running.get()) {
            try {
                KafkaConsumer<String, String> consumer = ModerationUtils.createConsumer(
                        bootstrapServers, "dariabelll-shortener"
                );
                try {
                    consumer.subscribe(List.of("links.moderated"));
                    while (running.get()) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                        for (ConsumerRecord<String, String> record : records) {
                            if (!running.get()) {
                                break;
                            }
                            applyResult(record);
                            ModerationUtils.commitRecord(consumer, record, API_TIMEOUT);
                        }
                    }
                } finally {
                    closeConsumer(consumer);
                }
            } catch (IOException | RuntimeException e) {
                pauseAfterFailure();
            }
        }
    }

    private void applyResult(ConsumerRecord<String, String> record) throws IOException {
        boolean passed;
        try {
            ModerationResultEvent result = ModerationUtils.parseModerationResult(record.key(), record.value());
            passed = result.passed();
        } catch (IllegalArgumentException e) {
            return;
        }
        if (!passed) {
            urlDao.delete(record.key());
        }
    }

    private void pauseAfterFailure() {
        try {
            if (running.get()) {
                Thread.sleep(RETRY_DELAY_MILLIS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running.set(false);
        }
    }

    private static void closeProducer(KafkaProducer<String, String> producer) {
        producer.close(API_TIMEOUT);
    }

    private static void closeConsumer(KafkaConsumer<String, String> consumer) {
        consumer.close(CloseOptions.timeout(API_TIMEOUT));
    }

}
