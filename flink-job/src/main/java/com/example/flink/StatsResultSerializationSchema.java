package com.example.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.Serial;

/**
 * Превращает StatsResult в JSON для записи в Kafka.
 */
public class StatsResultSerializationSchema implements SerializationSchema<StatsResult> {

    @Serial
    private static final long serialVersionUID = 1L;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(StatsResult element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Ошибка сериализации StatsResult в JSON", e);
        }
    }
}
