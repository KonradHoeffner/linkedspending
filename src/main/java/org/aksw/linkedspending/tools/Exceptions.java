package org.aksw.linkedspending.tools;

/**
 * Custom exceptions
 */
public class Exceptions {

    static public class NoCurrencyFoundForCodeException extends Exception {
        public NoCurrencyFoundForCodeException(String datasetName, String code) {
            super("no currency found for code "+code+" in dataset "+datasetName);
        }
    }

    static public class DatasetHasNoCurrencyException extends Exception {
        public DatasetHasNoCurrencyException(String datasetName) {
            super("dataset "+datasetName+" has no currency.");
        }
    }

    static public class MissingDataException extends Exception {
        public MissingDataException(String s) {
            super(s);
        }
    }

    static public class UnknownMappingTypeException extends Exception {
        public UnknownMappingTypeException(String s) {
            super(s);
        }
    }

    static public class TooManyMissingValuesException extends Exception {
        public TooManyMissingValuesException(String datasetName, int i) {
            super(i+" missing values in dataset "+datasetName);
        }
    }
}