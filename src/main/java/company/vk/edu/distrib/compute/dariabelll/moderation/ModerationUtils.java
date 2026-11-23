package company.vk.edu.distrib.compute.dariabelll.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public final class ModerationUtils {

    private static final long SEND_MAX_BLOCK_MILLIS = 1000;
    private static final int CREATED_EVENT_FIELDS_COUNT = 3;
    private static final int RESULT_EVENT_FIELDS_COUNT = 4;

    private ModerationUtils() {
    }

    public static LinkCreatedEvent parseLinkCreatedEvent(@Nullable String key, @Nullable String value) {
        if (value == null) {
            throw new IllegalArgumentException("Event value is missing");
        }

        String[] fields = value.split("\t", -1);
        if (fields.length != CREATED_EVENT_FIELDS_COUNT) {
            throw new IllegalArgumentException("Expected id, encoded URL and creation time");
        }

        String id = fields[0];
        if (id.isBlank() || !id.equals(key)) {
            throw new IllegalArgumentException("Event ID must match the message key");
        }

        String url = new String(Base64.getDecoder().decode(fields[1]), StandardCharsets.UTF_8);
        if (url.isBlank()) {
            throw new IllegalArgumentException("URL is missing");
        }

        try {
            Instant createdAt = Instant.parse(fields[2]);
            return new LinkCreatedEvent(id, url, createdAt);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid creation time", e);
        }
    }

    public static String formatModerationResult(ModerationResultEvent event) {
        String encodedReason = Base64.getEncoder().encodeToString(event.reason().getBytes(StandardCharsets.UTF_8));
        return String.join(
                "\t", event.id(), event.passed().toString(), encodedReason, event.moderatedAt().toString()
        );
    }

    public static String formatLinkCreatedEvent(LinkCreatedEvent event) {
        String encodedUrl = Base64.getEncoder()
                .encodeToString(event.url().getBytes(StandardCharsets.UTF_8));

        return String.join(
                "\t",
                event.id(),
                encodedUrl,
                event.createdAt().toString()
        );
    }

    public static ModerationResultEvent parseModerationResult(@Nullable String key, @Nullable String value) {
        if (value == null) {
            throw new IllegalArgumentException("Moderation result is missing");
        }
        String[] fields = value.split("\t", -1);
        if (fields.length != RESULT_EVENT_FIELDS_COUNT || fields[0].isBlank() || !fields[0].equals(key)) {
            throw new IllegalArgumentException("Invalid moderation result fields or ID");
        }
        if (!"true".equals(fields[1]) && !"false".equals(fields[1])) {
            throw new IllegalArgumentException("Expected true or false as moderation decision");
        }
        String reason = new String(Base64.getDecoder().decode(fields[2]), StandardCharsets.UTF_8);
        try {
            return new ModerationResultEvent(
                    fields[0], Boolean.valueOf(fields[1]), reason, Instant.parse(fields[3])
            );
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid moderation time", e);
        }
    }

    public static List<String> loadBannedWords() throws IOException {
        try (InputStream input = ModerationUtils.class.getResourceAsStream("/moderation/banned_words.txt")) {
            if (input == null) {
                throw new IOException("Banned words resource not found");
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                return reader.lines()
                        .map(String::strip)
                        .filter(word -> !word.isEmpty())
                        .map(word -> word.toLowerCase(Locale.ROOT))
                        .toList();
            }
        }
    }

    public static void commitRecord(
            KafkaConsumer<String, String> consumer, ConsumerRecord<String, String> record, Duration timeout) {
        consumer.commitSync(Map.of(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        ), timeout);
    }

    public static KafkaProducer<String, String> createProducer(String bootstrapServers) {
        Properties settings = new Properties();
        settings.put("bootstrap.servers", bootstrapServers);
        settings.put("max.block.ms", SEND_MAX_BLOCK_MILLIS);
        return new KafkaProducer<>(settings, new StringSerializer(), new StringSerializer());
    }

    public static KafkaConsumer<String, String> createConsumer(String bootstrapServers, String groupId) {
        Properties settings = new Properties();
        settings.put("bootstrap.servers", bootstrapServers);
        settings.put("group.id", groupId);
        settings.put("auto.offset.reset", "earliest");
        settings.put("enable.auto.commit", false);
        return new KafkaConsumer<>(settings, new StringDeserializer(), new StringDeserializer());
    }

}
