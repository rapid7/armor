package com.rapid7.armor.read.predicate;

import java.util.List;

import com.rapid7.armor.store.Operator;

public class NumericPredicate<T extends Number> extends Predicate<T> {

    public NumericPredicate(String field, Operator operator, List<T> values) {
        super(field, operator, values);
    }

    public NumericPredicate(String field, Operator operator, T value) {
        super(field, operator, value);
    }

    @Override
    public boolean executeTest(T testValue) {
      if (operator == Operator.EQUALS) {
        return executeEquals(testValue);
      } else if (operator == Operator.NOT_EQUALS) {
        return executeNotEquals(testValue);
      } else if (operator == Operator.GREATER_THAN) {
        return executeNumGreaterThan(testValue);
      } else if (operator == Operator.GREATER_THAN_EQUAL) {
        return executeNumGreaterThanEqual(testValue);
      } else if (operator == Operator.LESS_THAN) {
        return executeNumLessThan(testValue);
      } else if (operator == Operator.LESS_THAN_EQUAL) {
        return executeNumLessThanEqual(testValue);
      } else if (operator == Operator.BETWEEN) {
        return executeNumBetween(testValue);
      } else if (operator == Operator.IN) {
        return executeIn(testValue);
      } else if (operator == Operator.IS_NULL) {
        return executeIsNull();
      } else if (operator == Operator.NOT_NULL) {
        return executeIsNotNull();
      }
      return false;
    }

    @Override
    public Number convertValueToNumber(T value) {
        return value;
    }
}
