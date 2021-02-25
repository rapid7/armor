package com.rapid7.armor.store;

import java.util.List;
import java.util.Objects;

public abstract class Predicate<T> {
   protected String field;
   protected Operator operator;
   protected List<T> values;
   
   public Predicate(String field, Operator operator, List<T> values) {
     this.field = field;
     this.operator = operator;
     this.values = Objects.requireNonNull(values, "Values parameter cannot be null");
   }
   
   public boolean test(List<T> testValues) {
       return executeTest(testValues);  
   }
   
   public abstract boolean executeTest(List<T> testValues);
   
   public abstract Number convertValuesToNumber(T value);
 
   protected boolean executeBetween(Number testValue) {
     if (values.size() != 2)
        throw new IllegalStateException("Expected only a 2 values in the predicate");
     Number a = convertValuesToNumber(values.get(0));
     Number b = convertValuesToNumber(values.get(1));
     return testValue.doubleValue() >= a.doubleValue() && testValue.doubleValue() <= b.doubleValue();
   }
   
   protected boolean executeIn(T testValue) {
      for (T value : values) {
        if (value.equals(testValue))
            return true;
      }
      return false;
   }

   protected boolean executeEquals(T testValue) {
      if (values.size() != 1)
        throw new IllegalStateException("Expected only a single value in the predicate");
      
      return testValue.equals(values.get(0));
   }
   
   protected boolean executeNotEquals(T testValue) {
       if (values.size() != 1)
         throw new IllegalStateException("Expected only a single value in the predicate");
       
       return !testValue.equals(values.get(0));
   }
   
   protected boolean executeGreaterThan(Number testValue) {
       if (values.size() != 1)
         throw new IllegalStateException("Expected only a single value in the predicate");
       
       return testValue.doubleValue() > convertValuesToNumber(values.get(0)).doubleValue();
   }
   
   protected boolean executeGreaterThanEqual(Number testValue) {
       if (values.size() != 1)
         throw new IllegalStateException("Expected only a single value in the predicate");
       
       return testValue.doubleValue() >= convertValuesToNumber(values.get(0)).doubleValue();
   }
   
   protected boolean executeLessThan(Number testValue) {
       if (values.size() != 1)
           throw new IllegalStateException("Expected only a single value in the predicate");
       
       return testValue.doubleValue() < convertValuesToNumber(values.get(0)).doubleValue();
   }
   
   protected boolean executeLessThanEqual(Number testValue) {
       if (values.size() != 1)
           throw new IllegalStateException("Expected only a single value in the predicate");
       
       return testValue.doubleValue() <= convertValuesToNumber(values.get(0)).doubleValue();
   }
}
