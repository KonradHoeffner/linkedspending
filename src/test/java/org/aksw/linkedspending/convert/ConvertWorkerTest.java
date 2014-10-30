package org.aksw.linkedspending.convert;

import org.aksw.linkedspending.download.DownloadWorker;
import org.aksw.linkedspending.job.Job;
import org.aksw.linkedspending.job.Job.DataSetDoesNotExistException;
import org.junit.Test;

public class ConvertWorkerTest
{

	@Test public void testConvert() throws DataSetDoesNotExistException
	{
		new DownloadWorker("2013", Job.forDataset("2013"),false).get();
		new ConvertWorker("2013", Job.forDataset("2013"),true).get();
	}

}