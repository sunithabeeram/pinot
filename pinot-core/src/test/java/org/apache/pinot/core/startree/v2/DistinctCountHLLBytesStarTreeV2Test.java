/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.startree.v2;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.util.Random;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.data.aggregator.DistinctCountHLLValueAggregator;
import org.apache.pinot.core.data.aggregator.ValueAggregator;
import org.apache.pinot.core.query.aggregation.function.DistinctCountHLLAggregationFunction;

import static org.testng.Assert.assertEquals;


public class DistinctCountHLLBytesStarTreeV2Test extends BaseStarTreeV2Test<Object, HyperLogLog> {

  @Override
  ValueAggregator<Object, HyperLogLog> getValueAggregator() {
    return new DistinctCountHLLValueAggregator();
  }

  @Override
  DataType getRawValueType() {
    return DataType.BYTES;
  }

  @Override
  Object getRandomRawValue(Random random) {
    return ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(new HyperLogLog(DistinctCountHLLAggregationFunction.DEFAULT_LOG2M));
  }

  @Override
  void assertAggregatedValue(HyperLogLog starTreeResult, HyperLogLog nonStarTreeResult) {
    System.out.println("starTreeResult " + starTreeResult.cardinality());
    assertEquals(starTreeResult.cardinality(), nonStarTreeResult.cardinality());
  }
}
