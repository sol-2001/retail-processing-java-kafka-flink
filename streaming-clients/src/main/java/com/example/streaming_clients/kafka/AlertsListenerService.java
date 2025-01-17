package com.example.streaming_clients.kafka;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Слушает топик "sales_alerts_1m".
 */
@Getter
@Service
@RequiredArgsConstructor
@Slf4j
public class AlertsListenerService {

    private final List<String> recentAlerts = new CopyOnWriteArrayList<>();

    @KafkaListener(
            topics = "sales_alerts_1m",
            groupId = "${spring.kafka.consumer.group-id:alerts-consumer-1}"
    )
    public void onAlertMessage(String message) {
        log.debug("[AlertsListener] Received from sales_alerts_1m: {}", message);
        recentAlerts.add(message);

        if (recentAlerts.size() > 100) {
            recentAlerts.remove(0);
        }
    }
}
