package com.linkedin.pinot.core.query.aggregation.function.customobject;

import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import javax.annotation.Nonnull;


public class MinMaxStringValue implements MinMaxValue<String> {


  private static final Charset UTF_8 = Charset.forName(V1Constants.Str.CHAR_SET);

  private String _value;

  public MinMaxStringValue(String value) {
    _value = value;
  }

  @Override
  public void setValue(String value) {
    _value = value;
  }

  @Override
  public String getValue() {
    return _value;
  }

  @Override
  public void applyMax(String otherValue) {
    if (otherValue.compareTo(_value) > 0) {
      _value = otherValue;
    }
  }

  @Override
  public void applyMin(String otherValue) {
    if (otherValue.compareTo(_value) < 0) {
      _value = otherValue;
    }
  }

  @Nonnull
  public byte[] toBytes() {
    byte[] valueBytes =_value.getBytes(UTF_8);
    return valueBytes;
  }

  @Nonnull
  public static MinMaxStringValue fromBytes(byte[] bytes) {
    String value = new String(bytes, UTF_8);
    return new MinMaxStringValue(value);
  }

  @Nonnull
  public static MinMaxStringValue fromByteBuffer(ByteBuffer byteBuffer) {
    byte[] bytes = new byte[byteBuffer.limit()];
    byteBuffer.get(bytes);
    String value = new String(bytes, UTF_8);
    return new MinMaxStringValue(value);
  }

  @Override
  public String toString() {
    return _value;
  }

  @Override
  public int compareTo(MinMaxValue<String> o) {
    MinMaxStringValue other = (MinMaxStringValue) o;
    return this.getValue().compareTo(other.getValue());
  }
}
