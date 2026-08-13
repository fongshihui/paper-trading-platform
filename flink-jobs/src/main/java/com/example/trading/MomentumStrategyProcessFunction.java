package com.example.trading;

import java.time.Instant;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Momentum-based trading strategy implemented as a Flink ProcessFunction.
 */
public class MomentumStrategyProcessFunction
        extends KeyedProcessFunction<String, MarketData, Signal> {

    private final double threshold;
    private final int quantity;
    private transient ValueState<Double> lastPrice;

    public MomentumStrategyProcessFunction(double threshold, int quantity) {
        this.threshold = threshold;
        this.quantity = quantity;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> descriptor =
                new ValueStateDescriptor<>("lastPrice", TypeInformation.of(Double.class));
        lastPrice = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            MarketData value, Context ctx, Collector<Signal> out) throws Exception {
        Double prev = lastPrice.value();

        if (prev == null || prev <= 0) {
            lastPrice.update(value.getPrice());
            return;
        }

        double change = (value.getPrice() - prev) / prev;
        String side = null;

        if (change > threshold) {
            side = "BUY";
        } else if (change < -threshold) {
            side = "SELL";
        }

        lastPrice.update(value.getPrice());

        if (side != null) {
            out.collect(
                    new Signal(
                            value.getSymbol(),
                            side,
                            value.getPrice(),
                            quantity,
                            Instant.now().toString()));
        }
    }
}
