package com.linkedin.pinot.core.query.aggregation.function.customobject;

public interface MinMaxValue<T> extends Comparable<MinMaxValue<T>> {

  void setValue(T value);

  T getValue();

  void applyMax(T otherValue);

  void applyMin(T otherValue);

}
