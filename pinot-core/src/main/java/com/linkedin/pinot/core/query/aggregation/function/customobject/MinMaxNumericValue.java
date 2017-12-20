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
package com.linkedin.pinot.core.query.aggregation.function.customobject;

import com.google.common.math.DoubleMath;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.nio.ByteBuffer;
import java.util.Locale;
import javax.annotation.Nonnull;


public class MinMaxNumericValue implements MinMaxValue<Double> {

  private Double _value;

  public MinMaxNumericValue(Double value) {
    _value = value;
  }

  @Override
  public void setValue(Double value) {
    _value = value;
  }

  @Override
  public Double getValue() {
    return _value;
  }

  @Override
  public void applyMax(Double otherValue) {
    if (otherValue.compareTo(_value) > 0) {
      _value = otherValue;
    }
  }

  @Override
  public void applyMin(Double otherValue) {
    if (otherValue.compareTo(_value) < 0) {
      _value = otherValue;
    }
  }

  @Nonnull
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(V1Constants.Numbers.DOUBLE_SIZE);
    byteBuffer.putDouble(_value);
    return byteBuffer.array();
  }

  @Nonnull
  public static MinMaxNumericValue fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @Nonnull
  public static MinMaxNumericValue fromByteBuffer(ByteBuffer byteBuffer) {
    return new MinMaxNumericValue(byteBuffer.getDouble());
  }

  @Override
  public String toString() {
    if (_value <= Long.MAX_VALUE && DoubleMath.isMathematicalInteger(_value)) {
      return Long.toString(_value.longValue()) + ".00000";
    } else {
      return String.format(Locale.US, "%1.5f", _value);
    }
  }

  @Override
  public int compareTo(MinMaxValue<Double> o) {
    MinMaxNumericValue other = (MinMaxNumericValue) o;
    return this.getValue().compareTo(other.getValue());
  }
}
