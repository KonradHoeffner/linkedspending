package org.aksw.linkedspending;

import org.aksw.linkedspending.downloader.JsonDownloader;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonDownloaderTest
{
    @Test
    public void testGetPartialResults() throws IOException
    {
        try {
            JsonDownloader.downloadSpecific("2013");
        } catch (Exception e) {
            fail("Could not download dataset: " + e.getMessage());
        }
    }

    @Test
    public void testGetDatasetNames() throws IOException
    {
        Collection<String> names = JsonDownloader.getDatasetNames();
        assertTrue(names.size()>300);
        assertTrue(names.contains("berlin_de"));
    }
}