package org.aksw.linkedspending.exception;

public class TooManyMissingValuesException extends Exception
	{
		/**
	 * 
	 */
	private static final long	serialVersionUID	= 1L;

		public TooManyMissingValuesException(String datasetName, int i)
		{
			super(i + " missing values in dataset " + datasetName);
		}
	}