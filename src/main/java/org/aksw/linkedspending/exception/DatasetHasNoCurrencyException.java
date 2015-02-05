package org.aksw.linkedspending.exception;

public class DatasetHasNoCurrencyException extends Exception
{
	/**
	 * 
	 */
	private static final long	serialVersionUID	= 1L;

	public DatasetHasNoCurrencyException(String datasetName)
	{
		super("dataset " + datasetName + " has no currency.");
	}
}