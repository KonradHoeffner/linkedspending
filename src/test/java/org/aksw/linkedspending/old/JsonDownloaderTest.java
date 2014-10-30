package org.aksw.linkedspending.old;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.aksw.linkedspending.DatasetInfo;
import org.aksw.linkedspending.job.Job;
import org.aksw.linkedspending.old.JsonDownloaderOld;
import org.junit.Before;
import org.junit.Test;

public class JsonDownloaderTest
{
	@Before public void before()
	{
		JsonDownloaderOld.setStopRequested(false);
	}

	private final String	DATASET	= "2013";

	@Test public void testGetPartialResults() throws IOException
	{
		try
		{
			JsonDownloaderOld.downloadSpecificOld(DATASET);
		}
		catch (Exception e)
		{
			fail("Could not download dataset: " + e.getMessage());
		}
		File test = new File("json/parts/" + DATASET + "/" + DATASET + ".final");
		assertTrue("JSON file not existing", test.exists());
	}

	@Test public void testGetDatasetNames() throws IOException
	{
		Map<String,DatasetInfo> infos = JsonDownloaderOld.getDatasetInfosCached();
		assertTrue(infos.size() > 700);
		assertTrue(infos.containsKey(DATASET));
	}
}