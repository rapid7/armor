package com.rapid7.armor.read.predicate;

import java.util.Arrays;
import java.util.List;

import com.rapid7.armor.store.Operator;

public abstract class Predicate<T> {
   protected String field;
   protected Operator operator;
   protected List<T> values;
   
   public Predicate(String field, Operator operator, List<T> values) {
     this.field = field;
     this.operator = operator;
     this.values = values;
   }
   
   /**
    * Constructs a predicate.
    *
    * @param field The name of the field.
    * @param operator The operator to execute the against the field.
    * @param value The predicate values not the test against values.
    */
   public Predicate(String field, Operator operator, T value) {
     this.field = field;
     this.operator = operator;
     this.values = Arrays.asList(value);
   }
   
   public boolean test(T testValue) {
     return executeTest(testValue);  
   }
   
   public T getValue() {
     if (values == null || values.isEmpty())
       return null;
     return values.get(0);
   }

   public Operator getOperator() {
       return operator;
   }
   
   public abstract boolean executeTest(T testValue);
   
   public abstract Number convertValueToNumber(T value);
 
   protected boolean executeLexBetween(T testValue) {
     if (values.size() != 2)
       throw new RuntimeException("You must have two values setup in this predicate to execute between");
     if (testValue == null)
       return false;
     
     String a = values.get(0) == null ? null : values.get(0).toString();
     String b = values.get(1) == null ? null : values.get(1).toString();

     if (a == null || b == null)
       return false;
 
     String test = testValue.toString();
     int aResult = a.compareTo(test);
     int bResult = b.compareTo(test);
     return aResult <= 0 && bResult >= 0;
   }
   
   protected boolean executeNumBetween(T testValue) {
     if (values == null || values.size() != 2)
       throw new RuntimeException("You must have two values to execute between predicate");
     Number fieldNumber = convertValueToNumber(testValue);
     Number aNum = convertValueToNumber(values.get(0));
     Number bNum = convertValueToNumber(values.get(1));
     return fieldNumber.doubleValue() >= aNum.doubleValue() && fieldNumber.doubleValue() <= bNum.doubleValue();
   }
   
   protected boolean executeIn(T testValue) {
      if (testValue == null)
        return false;
      for (T value : values) {
        if (testValue != null && testValue.equals(value))
          return true;
      }
      return false;
   }

   protected boolean executeEquals(T testValue) {
     if (testValue == null)
       return false;
     T value = getSingleValue(values);
     return testValue.equals(value);
   }
   
   protected boolean executeNotEquals(T testValue) {
     if (testValue == null)
       return false;
     T value = getSingleValue(values);
     return !testValue.equals(value);
   }
   
   protected boolean executeLexGreaterThan(String testValue) {
     if (testValue == null)
       return false;
     T value = getSingleValue(values);
     if (value == null)
       return false;
     int result = testValue.compareTo(value.toString());
     return result > 0;
   }
     
   protected boolean executeLexGreaterThanEqual(String testValue) {
     if (testValue == null)
       return false;
     T value = getSingleValue(values);
     if (value == null)
       return false;
     int result = testValue.compareTo(value.toString());
     return result >= 0;
   }
     
   protected boolean executeLexLessThan(String testValue) {
     if (testValue == null)
       return false;
     T value = getSingleValue(values);
     if (value == null)
       return false;
     int result = testValue.compareTo(value.toString());
     return result < 0;
   }
     
   protected boolean executeLexLessThanEqual(String testValue) {
     if (testValue == null)
       return false;
     T value = getSingleValue(values);
     if (value == null)
       return false;
     int result = testValue.compareTo(value.toString());
     return result <= 0;
   }
   
   protected boolean executeNumGreaterThan(T testValue) {
     if (testValue == null)
        return false;
     T value = getSingleValue(values);
     if (value == null)
        return false;
     return convertValueToNumber(testValue).doubleValue() > convertValueToNumber(value).doubleValue();
   }
   
   protected boolean executeNumGreaterThanEqual(T testValue) {
     if (testValue == null)
        return false;
     T value = getSingleValue(values);
     if (value == null)
        return false;
     return convertValueToNumber(testValue).doubleValue() >= convertValueToNumber(value).doubleValue();
   }
   
   protected boolean executeNumLessThan(T testValue) {
     if (testValue == null)
       return false;
     T value = getSingleValue(values);
     if (value == null)
       return false;
     return convertValueToNumber(testValue).doubleValue() < convertValueToNumber(value).doubleValue();
   }
   
  protected boolean executeNumLessThanEqual(T testValue) {
    if (testValue == null)
      return false;
    T value = getSingleValue(values);
    if (value == null)
      return false;
    return convertValueToNumber(testValue).doubleValue() <= convertValueToNumber(value).doubleValue();
  }
   
  private Number getSingleValue(Number... values) {
    if (values == null)
      return null;
    if (values.length == 0)
      return null;
    return values[0];
  }

  private String getSingleValue(String... values) {
    if (values == null)
      return null;
    if (values.length == 0)
      return null;
    return values[0];
  }
   
  private T getSingleValue(List<T> values) {
    if (values == null)
      return null;
    if (values.isEmpty())
      return null;
    return values.get(0);
  }
}
