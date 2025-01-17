package com.example.retail_processing;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class SalesData {
    private String purchaseID;
    private String productID;
    private double price;
    private String timeStamp;
    private String storeID;
}