package org.aksw.linkedspending.exception;

public class DataSetDoesNotExistException extends RuntimeException
{
	public DataSetDoesNotExistException(String datasetName)
	{
		super("dataset \""+datasetName+"\" is not present at OpenSpending (in case of old cache, refresh it).");
	}
}