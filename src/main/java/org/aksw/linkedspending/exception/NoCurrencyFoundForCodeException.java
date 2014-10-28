package org.aksw.linkedspending.exception;

public class NoCurrencyFoundForCodeException extends Exception
{
	public NoCurrencyFoundForCodeException(String datasetName, String code)
	{
		super("no currency found for code " + code + " in dataset " + datasetName);
	}
}