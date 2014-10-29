package org.aksw.linkedspending.job;

import static org.junit.Assert.*;
import org.aksw.linkedspending.job.Job.DataSetDoesNotExistException;
import org.junit.Test;
import de.konradhoeffner.commons.MemoryBenchmark;

public class ProcessorTest
{

	@Test public void testProcess() throws DataSetDoesNotExistException
	{
			new Processor("2013",Job.forDataset("2013")).process();
	}

}
