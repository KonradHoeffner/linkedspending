package org.aksw.linkedspending.exception;

public class TooManyMissingValuesException extends Exception
	{
		public TooManyMissingValuesException(String datasetName, int i)
		{
			super(i + " missing values in dataset " + datasetName);
		}
	}