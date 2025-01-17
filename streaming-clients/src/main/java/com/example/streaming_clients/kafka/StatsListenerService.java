package com.example.streaming_clients.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Слушает топик "sales_stats_1m".
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class StatsListenerService {

    @Getter
    private final List<String> recentStats = new CopyOnWriteArrayList<>();


    @KafkaListener(
            topics = "sales_stats_1m",
            groupId = "${spring.kafka.consumer.group-id:stats-consumer-1}"
    )
    public void onStatsMessage(String message) {
        log.debug("[StatsListener] Received from sales_stats_1m: {}", message);
        recentStats.add(message);
        if (recentStats.size() > 100) {
            recentStats.remove(0);
        }
    }
}
