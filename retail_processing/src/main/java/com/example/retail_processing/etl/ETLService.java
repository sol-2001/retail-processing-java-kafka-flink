package com.example.retail_processing.etl;

import com.example.retail_processing.DataGenerator;
import com.example.retail_processing.SalesData;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ThreadLocalRandom;


@Service
@RequiredArgsConstructor
@Slf4j
public class ETLService {

    private final KafkaProducerService producerService;
    private final DataGenerator dataGenerator;

    private int minDelaySeconds = 2;

    private int maxDelaySeconds = 20;

    /**
     * Метод, который генерирует заказ и отправляет его в Kafka.
     */
    @Transactional
    @Scheduled(fixedDelayString = "${data.generator.sleep.millis:1000}")
    public void processAndSend() {
        // Генерация заказа
        SalesData sale = dataGenerator.generateSale();
        try {
            producerService.sendMessage("preprocessed_sales", sale);
            log.debug("Отправлен заказ: {}", sale);
        } catch (JsonProcessingException e) {
            log.error("Ошибка сериализации сообщения: {}", sale, e);
        }

        // Случайная задержка перед следующей отправкой
        int randomDelay = ThreadLocalRandom.current().nextInt(minDelaySeconds, maxDelaySeconds + 1) * 1000;
        try {
            log.info("Следующий заказ будет отправлен через {} миллисекунд", randomDelay);
            Thread.sleep(randomDelay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
