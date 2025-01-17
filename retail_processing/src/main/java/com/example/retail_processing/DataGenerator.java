package com.example.retail_processing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
@Slf4j
public class DataGenerator {

    private final Random random = new Random();
    private final List<String> stores = List.of("SPB_01", "MSC_01", "NSK_01");

    private final Map<String, Double> products = new HashMap<>() {{
        put("P1001", 250.0);
        put("P1002", 800.0);
        put("P1003", 1200.0);
        put("P1004", 99.0);
        put("P1005", 3000.0);
        put("P1006", 45.0);
        put("P1007", 1500.0);
    }};


    public SalesData generateSale() {
        String storeId = stores.get(random.nextInt(stores.size()));

        List<String> productIds = new ArrayList<>(products.keySet());
        String productId = productIds.get(random.nextInt(productIds.size()));
        String purchaseId = UUID.randomUUID().toString();

        String timeStamp = LocalDateTime.now()
                .format(DateTimeFormatter.ISO_DATE_TIME);

        double basePrice = products.get(productId);
        double variationFactor = 1.0 + (random.nextDouble() * 0.2 - 0.1);
        double price = basePrice * variationFactor;
        price = Math.round(price * 100.0) / 100.0;

        return new SalesData(
                purchaseId,
                productId,
                price,
                timeStamp,
                storeId
        );
    }
}
