import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.aksw.linkedspending.JsonDownloader;
import org.junit.Test;

import java.io.*;
import java.util.Collection;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonDownloaderTest
{
	@Test public void testGetPartialResults() throws JsonProcessingException, IOException
	{
		JsonDownloader.ResultsReader in = new JsonDownloader.ResultsReader("2013");
		JsonNode node;
		while((node=in.read())!=null ){}
		fail("not yet finished writing this test");
	}
		
	@Test public void testGetDatasetNames() throws IOException
	{
		Collection<String> names = JsonDownloader.getDatasetNames();
		assertTrue(names.size()>300);
		assertTrue(names.contains("berlin_de"));		
	}

    @Test public void testPuzzleTogether() throws Exception
    {
        File testfile1 = new File("json/parts/jsonDownloaderTestFile1");
        File testfile2 = new File("json/parts/jsonDownloaderTestFile2");

        try (PrintWriter out = new PrintWriter(testfile1))
        {
            out.println("\"results\": [");
            out.print("{\nhallo\n}");
        }
        try (PrintWriter out = new PrintWriter(testfile2))
        {
            out.println("\"results\": [");
            out.print("{\nwelt\n}");
        }

        //puzzleTogether

        String input= "";
        try(BufferedReader in = new BufferedReader(new FileReader(new File("json/model"))))
        {
            while((input += in.readLine())!=null){}
        }
        int worksfine = input.compareTo(
                "\"results\": [\n" +
                        "{\n" +
                        "hallo\n" +
                        "}," +
                        "\"results\": [\n" +
                        "{\n" +
                        "welt\n" +
                        "},");
        assertTrue(worksfine == 1);

        testfile1.delete();
        testfile2.delete();
    }

}