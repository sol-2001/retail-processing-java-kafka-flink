package com.example.flink;

import lombok.Data;

/**
 * Данные о продаже.
 */
@Data
public class SalesData {
    private String purchaseID;
    private String productID;
    private double price;
    private String timeStamp;
    private String storeID;
}
