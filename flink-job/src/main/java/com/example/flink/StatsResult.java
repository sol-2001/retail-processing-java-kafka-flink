package com.example.flink;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * Результат статистики за окно:
 * - количество продаж (count)
 * - средний чек (avgCheck)
 */
@Setter
@Getter
@Data
public class StatsResult {
    private String windowType;
    private int count;
    private double avgCheck;

    public StatsResult() {}

    public StatsResult(String windowType, int count, double avgCheck) {
        this.windowType = windowType;
        this.count = count;
        this.avgCheck = avgCheck;
    }

    @Override
    public String toString() {
        return String.format("StatsResult[%s, count=%d, avg=%.2f]",
                windowType, count, avgCheck);
    }
}
