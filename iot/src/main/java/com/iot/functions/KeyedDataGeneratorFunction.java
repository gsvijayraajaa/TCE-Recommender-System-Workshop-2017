package com.iot.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import com.iot.data.DataPoint;
import com.iot.data.KeyedDataPoint;

import java.util.LinkedHashMap;
import java.util.Map;

public class KeyedDataGeneratorFunction extends RichFlatMapFunction<DataPoint<Long>, KeyedDataPoint<Long>> {

  private static Map<String, Long> keyWeights() {
    Map<String, Long> weights = new LinkedHashMap<>();
    weights.put("amazon", 10L);
    weights.put("google", 50L);
    weights.put("facebook", 80L);
    return weights;
  }

  @Override
  public void flatMap(DataPoint<Long> dataPoint, Collector<KeyedDataPoint<Long>> collector) throws Exception {
    Map<String, Long> keyWeights = keyWeights();
    for (String key : keyWeights.keySet()) {
      for (int i = 0; i < keyWeights.get(key); i++) {
        KeyedDataPoint<Long> newData = new KeyedDataPoint<>(key, dataPoint.getTimeStampMs() + i, 1L);
        collector.collect(newData);
      }
    }
  }
}
