package org.aksw.linkedspending.exception;

public class MissingDataException extends Exception
{
	public MissingDataException(String dataset, String s)
	{
		super("missing data for dataset '"+dataset+"': "+s);
	}
}