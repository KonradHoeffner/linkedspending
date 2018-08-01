package org.aksw.linkedspending.downloader;

import java.io.File;
import java.io.IOException;
import org.aksw.linkedspending.SSL;
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
		SSL.disableSslVerification();
		//new DownloadWorker("2013", Job.forDatasetOrCreate("2013"),true).get();
		new DownloadWorker("open_bzobb", Job.forDatasetOrCreate("open_bzobb"),false).get();
		// throws an exception if the json file is invalid
		new ObjectMapper().readTree(new File("json/open_bzobb.json"));
	}

}