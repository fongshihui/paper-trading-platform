package com.example.trading;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * MarketData represents a single price tick.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MarketData {
    private String symbol;
    private double price;
    private String timestamp;
}
