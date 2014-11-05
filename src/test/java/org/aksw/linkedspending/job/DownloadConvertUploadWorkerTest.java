package org.aksw.linkedspending.job;

import org.aksw.linkedspending.exception.DataSetDoesNotExistException;
import org.junit.Test;

public class DownloadConvertUploadWorkerTest
{

	@Test public void testGet() throws DataSetDoesNotExistException
	{
		Job job = Job.forDataset("2013");
		new DownloadConvertUploadWorker("2013",job,true).get();
	}

}