/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.function.customobject.MinMaxNumericValue;
import com.linkedin.pinot.core.query.aggregation.function.customobject.MinMaxStringValue;
import com.linkedin.pinot.core.query.aggregation.function.customobject.MinMaxValue;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;

import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import javax.annotation.Nonnull;


public class MaxAggregationFunction implements AggregationFunction<MinMaxValue, MinMaxValue> {
  private static final String NAME = AggregationFunctionFactory.AggregationFunctionType.MAX.getName();

  @Nonnull
  @Override
  public String getName() {
    return NAME;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String[] columns) {
    return NAME + "_" + columns[0];
  }

  @Override
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity, trimSize);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    DataType dataType = blockValSets[0].getValueType();
    MinMaxValue maxValue;
    switch (dataType) {
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case DOUBLE:
        double[] doubleValueArray = blockValSets[0].getDoubleValuesSV();
        double maxDouble = Double.MIN_VALUE;
        for (int i = 0; i < length; i++) {
          double value = doubleValueArray[i];
          if (value > maxDouble) {
            maxDouble = value;
          }
        }
        maxValue = new MinMaxNumericValue(maxDouble);
        break;

      case STRING:
        String[] stringValueArray = blockValSets[0].getStringValuesSV();
        String maxString = "";
        for (int i = 0; i < length; i++) {
          String value = stringValueArray[i];
          if (value.compareTo(maxString) > 0) {
            maxString = value;
          }
        }
        maxValue = new MinMaxStringValue(maxString);
        break;

      default:
        throw new IllegalArgumentException("Max operation not supported on datatype " + dataType);
    }
    setAggregationResult(aggregationResultHolder, maxValue);
  }

  protected void setAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder, MinMaxValue maxValue) {
    MinMaxValue prevMaxValue = aggregationResultHolder.getResult();
    if (prevMaxValue == null) {
      aggregationResultHolder.setValue(maxValue);
    } else {
      prevMaxValue.applyMax(maxValue.getValue());
    }
    aggregationResultHolder.setValue(maxValue);
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    DataType dataType = blockValSets[0].getValueType();
    switch (dataType) {
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case DOUBLE:
        double[] doubleValueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setGroupByResult(groupKeyArray[i], groupByResultHolder, new MinMaxNumericValue(doubleValueArray[i]));
        }
        break;
      case STRING:
        String[] stringValueArray = blockValSets[0].getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setGroupByResult(groupKeyArray[i], groupByResultHolder, new MinMaxStringValue(stringValueArray[i]));
        }
        break;
      default:
        throw new IllegalArgumentException("Max operation not supported on datatype " + dataType);
    }

  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    DataType dataType = blockValSets[0].getValueType();
    switch (dataType) {
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case DOUBLE:
        double[] doubleValueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = doubleValueArray[i];
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, new MinMaxNumericValue(value));
          }
        }
        break;
      case STRING:
        String[] stringValueArray = blockValSets[0].getStringValuesSV();
        for (int i = 0; i < length; i++) {
          String value = stringValueArray[i];
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, new MinMaxStringValue(value));
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Max operation not supported on datatype " + dataType);
    }
  }

  protected void setGroupByResult(int groupKey, @Nonnull GroupByResultHolder groupByResultHolder, MinMaxValue maxValue) {
    MinMaxValue prevMaxValue = groupByResultHolder.getResult(groupKey);
    if (prevMaxValue == null) {
      groupByResultHolder.setValueForKey(groupKey, maxValue);
    } else {
      prevMaxValue.applyMax(maxValue.getValue());
    }
  }

  @Nonnull
  @Override
  public MinMaxValue extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Nonnull
  @Override
  public MinMaxValue extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Nonnull
  @Override
  public MinMaxValue merge(@Nonnull MinMaxValue intermediateResult1, @Nonnull MinMaxValue intermediateResult2) {
    if (intermediateResult1.compareTo(intermediateResult2) > 0) {
      return intermediateResult1;
    } else {
      return intermediateResult2;
    }
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return true;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getIntermediateResultDataType() {
    return FieldSpec.DataType.OBJECT;
  }

  @Nonnull
  @Override
  public MinMaxValue extractFinalResult(@Nonnull MinMaxValue intermediateResult) {
    return intermediateResult;
  }
}
