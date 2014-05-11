package org.aksw.linkedspending;

import com.fasterxml.jackson.databind.JsonNode;
import org.aksw.linkedspending.converter.ResultsReader;
import org.aksw.linkedspending.downloader.JsonDownloader;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

public class JsonDownloaderTest
{
    //@Test
    public void testGetPartialResults() throws IOException
    {
        ResultsReader in = new ResultsReader("2013");
        JsonNode node;
        while((node=in.read())!=null ){}
        //fail("not yet finished writing this test");
    }

    @Test
    public void testGetDatasetNames() throws IOException
    {
        Collection<String> names = JsonDownloader.getDatasetNames();
        assertTrue(names.size()>300);
        assertTrue(names.contains("berlin_de"));
    }
}