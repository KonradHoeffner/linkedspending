package org.aksw.linkedspending;

import static org.junit.Assert.assertTrue;
import java.time.Instant;
import lombok.val;
import org.aksw.linkedspending.exception.DataSetDoesNotExistException;
import org.aksw.linkedspending.job.DownloadConvertUploadWorker;
import org.aksw.linkedspending.job.Job;
import org.junit.Test;

public class LinkedSpendingDatasetInfoTest
{
	private static final String	name	= "2013";

	@Test public void testAll()
	{
		val infos = LinkedSpendingDatasetInfo.all();

	}

	@Test public void testForDataset() throws DataSetDoesNotExistException
	{
		// make sure dataset "2013" exists
		assertTrue(new DownloadConvertUploadWorker(name,Job.forDataset(name),false).get());
		LinkedSpendingDatasetInfo info = LinkedSpendingDatasetInfo.forDataset(name).get();
		assertTrue(info.name.equals(name));
		assertTrue(info.created.isBefore(Instant.now()));
//		System.out.println(info);
	}

}