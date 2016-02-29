package org.aksw.linkedspending.downloader;

import java.io.File;
import java.io.IOException;
import org.aksw.linkedspending.download.DownloadWorker;
import org.aksw.linkedspending.exception.DataSetDoesNotExistException;
import org.aksw.linkedspending.exception.MissingDataException;
import org.aksw.linkedspending.job.Job;
import org.junit.Test;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DownloadWorkerTest
{

	@Test public void testCall() throws IOException, InterruptedException, MissingDataException, DataSetDoesNotExistException
	{
		//new DownloadWorker("2013", Job.forDatasetOrCreate("2013"),true).get();
		new DownloadWorker("2012_tax", Job.forDatasetOrCreate("2012_tax"),false).get();
		// throws an exception if the json file is invalid
		new ObjectMapper().readTree(new File("json/2012_tax.json"));
	}

}