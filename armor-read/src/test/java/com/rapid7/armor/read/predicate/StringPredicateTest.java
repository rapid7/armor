package com.rapid7.armor.read.predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.rapid7.armor.store.Operator;

public class StringPredicateTest {

    @Test
    public void executePredTest() {
       StringPredicate equals = new StringPredicate("dummy", Operator.EQUALS, "dummy");
       assertTrue(equals.test("dummy"));
       assertFalse(equals.test("Dummy"));
       
       StringPredicate notequals = new StringPredicate("dummy", Operator.NOT_EQUALS, "dummy");
       assertFalse(notequals.test("dummy"));
       assertTrue(notequals.test("Dummy"));

       StringPredicate betweenA = new StringPredicate("dummy", Operator.BETWEEN, Arrays.asList("a", "c"));
       assertTrue(betweenA.test("b"));
       
       StringPredicate betweenB = new StringPredicate("dummy", Operator.BETWEEN, Arrays.asList("c", "a"));
       assertFalse(betweenB.test("b"));
       
       StringPredicate gt = new StringPredicate("dummy", Operator.GREATER_THAN, "b");
       assertFalse(gt.test("a"));
       assertFalse(gt.test("b"));
       assertTrue(gt.test("c"));
       
       StringPredicate gte = new StringPredicate("dummy", Operator.GREATER_THAN_EQUAL, "b");
       assertFalse(gte.test("a"));
       assertTrue(gte.test("b"));
       assertTrue(gte.test("c"));
       
       StringPredicate lt = new StringPredicate("dummy", Operator.LESS_THAN, "b");
       assertTrue(lt.test("a"));
       assertFalse(lt.test("b"));
       assertFalse(lt.test("c"));
       
       StringPredicate lte = new StringPredicate("dummy", Operator.LESS_THAN_EQUAL, "b");
       assertTrue(lte.test("a"));
       assertTrue(lte.test("b"));
       assertFalse(lte.test("c"));
    }
}
