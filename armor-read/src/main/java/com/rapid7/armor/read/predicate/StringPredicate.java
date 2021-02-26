package com.rapid7.armor.read.predicate;

import java.util.List;

import com.rapid7.armor.store.Operator;

public class StringPredicate extends Predicate<String> {
    public StringPredicate(String field, Operator operator, List<String> values) {
        super(field, operator, values);
    }
 
    public StringPredicate(String field, Operator operator, String value) {
        super(field, operator, value);
    }

    @Override
    public boolean executeTest(String testValue) {
      if (operator == Operator.EQUALS) {
        return executeEquals(testValue);
      } else if (operator == Operator.NOT_EQUALS) {
        return executeNotEquals(testValue);
      } else if (operator == Operator.BETWEEN) {
        return executeLexBetween(testValue);
      } else if (operator == Operator.IN) {
        return executeIn(testValue);
      } else if (operator == Operator.GREATER_THAN) {
        return executeLexGreaterThan(testValue);
      } else if (operator == Operator.GREATER_THAN_EQUAL) {
        return executeLexGreaterThanEqual(testValue);
      } else if (operator == Operator.LESS_THAN) {
        return executeLexLessThan(testValue);
      } else if (operator == Operator.LESS_THAN_EQUAL) {
        return executeLexLessThanEqual(testValue);
      }
      return false;
    }

    @Override
    public Number convertValueToNumber(String value) {
        throw new UnsupportedOperationException("Cannot use convert to number for StringPredicate");
    }
}
