package com.iot.functions;

import org.apache.flink.api.common.functions.RichMapFunction;

import com.iot.data.DataPoint;

/*
 * Expects a sawtooth wave as input!
 */
public class SquareWaveFunction extends RichMapFunction<DataPoint<Double>, DataPoint<Double>> {
  @Override
  public DataPoint<Double> map(DataPoint<Double> dataPoint) throws Exception {
    double value = 0.0;
    if(dataPoint.getValue() > 0.4){
      value = 1.0;
    }
    return dataPoint.withNewValue(value);
  }
}
