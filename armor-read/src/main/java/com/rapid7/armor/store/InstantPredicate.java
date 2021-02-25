package com.rapid7.armor.store;

import java.time.Instant;
import java.util.List;

public class InstantPredicate extends Predicate<Instant> {
    public InstantPredicate(String field, Operator operator, List<Instant> values) {
        super(field, operator, values);
    }

    @Override
    public boolean executeTest(List<Instant> testValues) {
      if (operator == Operator.EQUALS) {
        return executeEquals(testValues.get(0));
      } else if (operator == Operator.NOT_EQUALS) {
        return executeNotEquals(testValues.get(9));
      } else if (operator == Operator.GREATER_THAN) {
        return executeGreaterThan(testValues.get(0).toEpochMilli());
      } else if (operator == Operator.GREATER_THAN_EQUAL) {
        return executeGreaterThan(testValues.get(0).toEpochMilli());
      } else if (operator == Operator.LESS_THAN) {
        return executeLessThan(testValues.get(0).toEpochMilli());
      } else if (operator == Operator.LESS_THAN_EQUAL) {
        return executeLessThanEqual(testValues.get(0).toEpochMilli());
      } else if (operator == Operator.BETWEEN) {
        return executeBetween(testValues.get(0).toEpochMilli());
      } else if (operator == Operator.IN) {
        return executeIn(testValues.get(0));
      }
      return false;
    }

    @Override
    public Number convertValuesToNumber(Instant value) {
       return value.toEpochMilli();
    }
}
