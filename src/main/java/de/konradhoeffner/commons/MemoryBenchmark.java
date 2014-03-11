package de.konradhoeffner.commons;

/** @author konrad
Provides a rough approximation of the amount of memory used by the jvm. */
public class MemoryBenchmark
{
	long maxMemoryBytes = 0;

	/** Only provides a rough approximation. Time consuming so don't use it too often. */
	public long updateAndGetMaxMemoryBytes()
	{
		System.gc();
		maxMemoryBytes = Math.max(maxMemoryBytes,Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
		return maxMemoryBytes;
	}
	
}