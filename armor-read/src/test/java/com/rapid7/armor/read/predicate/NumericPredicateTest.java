package com.rapid7.armor.read.predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import com.rapid7.armor.store.Operator;

public class NumericPredicateTest {

    @Test
    public void executePredTest() {
       NumericPredicate<Integer> equals = new NumericPredicate<>("dummy", Operator.EQUALS, 1);
       assertTrue(equals.test(1));
       assertFalse(equals.test(2));
       
       NumericPredicate<Integer> notequals = new NumericPredicate<>("dummy", Operator.NOT_EQUALS, 1);
       assertFalse(notequals.test(1));
       assertTrue(notequals.test(2));
       
       NumericPredicate<Integer> notequalsSet = new NumericPredicate<Integer>("dummy", Operator.NOT_EQUALS, Lists.newArrayList(1, 2));
       assertFalse(notequalsSet.test(1));
       assertFalse(notequalsSet.test(2));
       assertTrue(notequalsSet.test(3));

       NumericPredicate<Integer> betweenA = new NumericPredicate<Integer>("dummy", Operator.BETWEEN, Arrays.asList(1, 3));
       assertTrue(betweenA.test(2));
       
       NumericPredicate<Integer> betweenB = new NumericPredicate<Integer>("dummy", Operator.BETWEEN, Arrays.asList(3, 1));
       assertFalse(betweenB.test(2));
       
       NumericPredicate<Integer> gt = new NumericPredicate<Integer>("dummy", Operator.GREATER_THAN, 2);
       assertFalse(gt.test(1));
       assertFalse(gt.test(2));
       assertTrue(gt.test(3));
       
       NumericPredicate<Integer> gte = new NumericPredicate<Integer>("dummy", Operator.GREATER_THAN_EQUAL, 2);
       assertFalse(gte.test(1));
       assertTrue(gte.test(2));
       assertTrue(gte.test(3));
       
       NumericPredicate<Integer> lt = new NumericPredicate<Integer>("dummy", Operator.LESS_THAN, 2);
       assertTrue(lt.test(1));
       assertFalse(lt.test(2));
       assertFalse(lt.test(3));
       
       NumericPredicate<Integer> lte = new NumericPredicate<Integer>("dummy", Operator.LESS_THAN_EQUAL, 2);
       assertTrue(lte.test(1));
       assertTrue(lte.test(2));
       assertFalse(lte.test(3));
       
       NumericPredicate<Integer> nullT = new NumericPredicate<Integer>("dummy", Operator.IS_NULL, (Integer) null);
       assertTrue(nullT.test(null));
       assertTrue(nullT.test(1)); // Doesn't matter

       NumericPredicate<Integer> notNull = new NumericPredicate<Integer>("dummy", Operator.NOT_NULL, 1);
       assertTrue(notNull.test(null));
    }
}
