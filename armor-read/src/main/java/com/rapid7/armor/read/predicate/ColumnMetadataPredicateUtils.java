package com.rapid7.armor.read.predicate;

/**
 * Utility class that provides functions to make it easy to figure out if a column may contains a certain value. 
 */
public final class ColumnMetadataPredicateUtils {
    private ColumnMetadataPredicateUtils() {}

    public static boolean columnMayContain(double testValue, double min, double max) {
        return testValue >= min && testValue <= max;
    }
    
    public static boolean columnMayHaveValueGreaterThan(double testValue, double max) {
        return testValue <= max;
    }
    
    public static boolean columnMayHaveValueLessThan(double testValue, double min) {
        return testValue >= min;
    }
}
