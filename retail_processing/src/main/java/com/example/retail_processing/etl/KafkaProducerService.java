package com.example.retail_processing.etl;

import com.example.retail_processing.SalesData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void sendMessage(String topic, SalesData salesData) throws JsonProcessingException {
        Map<String, List<SalesData>> payload = Collections.singletonMap("sales_info", Collections.singletonList(salesData));
        String message = objectMapper.writeValueAsString(payload);
        kafkaTemplate.send(topic, salesData.getPurchaseID(), message);
    }
}
