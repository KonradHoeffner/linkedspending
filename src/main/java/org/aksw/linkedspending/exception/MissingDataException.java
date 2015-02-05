package org.aksw.linkedspending.exception;

public class MissingDataException extends Exception
{
	/**
	 * 
	 */
	private static final long	serialVersionUID	= 1L;

	public MissingDataException(String dataset, String s)
	{
		super("missing data for dataset '"+dataset+"': "+s);
	}
}