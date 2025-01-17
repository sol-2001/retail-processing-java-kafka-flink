package com.example.streaming_clients.web;

import com.example.streaming_clients.kafka.AlertsListenerService;
import com.example.streaming_clients.kafka.StatsListenerService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.CopyOnWriteArrayList;

// SSE Контроллер для статистики и алёртов
@RestController
@RequestMapping("/sse")
@RequiredArgsConstructor
@Slf4j
public class StatsSseController {

    private final StatsListenerService statsService;
    private final AlertsListenerService alertsService;

    // Храним список активных SSE-подключений для stats и alerts
    private final List<SseEmitter> statsEmitters = new CopyOnWriteArrayList<>();
    private final List<SseEmitter> alertsEmitters = new CopyOnWriteArrayList<>();

    private ScheduledExecutorService scheduler;

    @GetMapping(value = "/stats", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamStats() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);

        // Добавляем в список активных
        statsEmitters.add(emitter);

        emitter.onCompletion(() -> statsEmitters.remove(emitter));
        emitter.onTimeout(() -> {
            statsEmitters.remove(emitter);
            emitter.complete();
        });
        emitter.onError((ex) -> {
            statsEmitters.remove(emitter);
            emitter.complete();
        });

        return emitter;
    }

    @GetMapping(value = "/alerts", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamAlerts() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);

        alertsEmitters.add(emitter);

        emitter.onCompletion(() -> alertsEmitters.remove(emitter));
        emitter.onTimeout(() -> {
            alertsEmitters.remove(emitter);
            emitter.complete();
        });
        emitter.onError((ex) -> {
            alertsEmitters.remove(emitter);
            emitter.complete();
        });

        return emitter;
    }

    @PostConstruct
    private void initScheduledPush() {
        scheduler = Executors.newScheduledThreadPool(2);

        // Каждые 3 секунды отправляем список recentStats всем клиентам
        scheduler.scheduleAtFixedRate(() -> {
            List<String> stats = statsService.getRecentStats();
            for (SseEmitter emitter : statsEmitters) {
                try {
                    emitter.send(stats, MediaType.APPLICATION_JSON);
                } catch (IOException e) {
                    log.warn("SSE stats error: {}", e.getMessage());
                    emitter.complete();
                    statsEmitters.remove(emitter);
                }
            }
        }, 0, 3, TimeUnit.SECONDS);

        // Каждые 3 секунды отправляем список recentAlerts всем клиентам
        scheduler.scheduleAtFixedRate(() -> {
            List<String> alerts = alertsService.getRecentAlerts();
            for (SseEmitter emitter : alertsEmitters) {
                try {
                    emitter.send(alerts, MediaType.APPLICATION_JSON);
                } catch (IOException e) {
                    log.warn("SSE alerts ошибка: {}", e.getMessage());
                    emitter.complete();
                    alertsEmitters.remove(emitter);
                }
            }
        }, 0, 3, TimeUnit.SECONDS);
    }

    @PreDestroy
    private void shutdownScheduler() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }
}
