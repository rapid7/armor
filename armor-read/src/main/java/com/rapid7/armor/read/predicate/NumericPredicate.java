package com.rapid7.armor.read.predicate;

import com.rapid7.armor.store.Operator;

public class NumericPredicate<T extends Number> extends Predicate<T> {

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
        return executeNumGreaterThan(testValue);
      } else if (operator == Operator.LESS_THAN) {
        return executeNumLessThan(testValue);
      } else if (operator == Operator.LESS_THAN_EQUAL) {
        return executeNumLessThanEqual(testValue);
      } else if (operator == Operator.BETWEEN) {
        return executeNumBetween(testValue);
      } else if (operator == Operator.IN) {
        return executeIn(testValue);
      }
      return false;
    }

    @Override
    public Number convertValueToNumber(T value) {
        return value;
    }
}
