package org.aksw.linkedspending.job;

import org.aksw.linkedspending.exception.DataSetDoesNotExistException;
import org.junit.Test;

public class DownloadConvertUploadWorkerTest
{

	@Test public void testGet() throws DataSetDoesNotExistException
	{
		Job job = Job.forDatasetOrCreate("finland-aid");
		new DownloadConvertUploadWorker("finland-aid",job,true).get();
	}

}