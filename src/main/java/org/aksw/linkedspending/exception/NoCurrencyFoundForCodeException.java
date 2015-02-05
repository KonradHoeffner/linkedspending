package org.aksw.linkedspending.exception;

public class NoCurrencyFoundForCodeException extends Exception
{
	/**
	 * 
	 */
	private static final long	serialVersionUID	= 1L;

	public NoCurrencyFoundForCodeException(String datasetName, String code)
	{
		super("no currency found for code " + code + " in dataset " + datasetName);
	}
}