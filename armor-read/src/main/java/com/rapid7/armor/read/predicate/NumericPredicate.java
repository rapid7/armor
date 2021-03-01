package com.rapid7.armor.read.predicate;

import com.rapid7.armor.store.Operator;

public class NumericPredicate<T extends Number> extends Predicate<T> {

    public NumericPredicate(String field, Operator operator, T value) {
        super(field, operator, value);
    }

    @Override
    public boolean executeTest(T testValue) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Number convertValueToNumber(T value) {
        // TODO Auto-generated method stub
        return null;
    }
}
