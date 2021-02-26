package com.rapid7.armor.read.predicate;

import java.time.Instant;
import java.util.List;

import com.rapid7.armor.store.Operator;

public class InstantPredicate extends Predicate<Instant> {
    public InstantPredicate(String field, Operator operator, List<Instant> values) {
        super(field, operator, values);
    }
    public InstantPredicate(String field, Operator operator, Instant value) {
        super(field, operator, value);
    }

    @Override
    public boolean executeTest(Instant testValue) {
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
    public Number convertValueToNumber(Instant value) {
       return value.toEpochMilli();
    }
}
