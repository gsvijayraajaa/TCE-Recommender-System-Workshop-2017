package com.iot.functions;

import org.apache.flink.api.common.functions.RichMapFunction;

import com.iot.data.DataPoint;

/*
 * Expects a sawtooth wave as input!
 */
public class SineWaveFunction extends RichMapFunction<DataPoint<Double>, DataPoint<Double>> {
  @Override
  public DataPoint<Double> map(DataPoint<Double> dataPoint) throws Exception {
    double phase = dataPoint.getValue() * 2 * Math.PI;
    return dataPoint.withNewValue(Math.sin(phase));
  }
}
