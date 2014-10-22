package org.aksw.linkedspending.downloader;

import org.aksw.linkedspending.downloader.JsonDownloader;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonDownloaderTest
{
	@Before public void before()
	{
		JsonDownloader.setStopRequested(false);
	}

	private final String	DATASET	= "2013";

	@Test public void testGetPartialResults() throws IOException
	{
		try
		{
			JsonDownloader.downloadSpecific(DATASET);
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
		Collection<String> names = JsonDownloader.getDatasetNamesCached();
		assertTrue(names.size() > 300);
		assertTrue(names.contains(DATASET));
	}
}