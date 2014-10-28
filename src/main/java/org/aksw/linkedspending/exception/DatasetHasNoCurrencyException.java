package org.aksw.linkedspending.exception;

public class DatasetHasNoCurrencyException extends Exception
{
	public DatasetHasNoCurrencyException(String datasetName)
	{
		super("dataset " + datasetName + " has no currency.");
	}
}