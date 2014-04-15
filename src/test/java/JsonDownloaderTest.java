import static org.junit.Assert.*;

import java.io.*;
import java.util.Collection;
import org.aksw.linkedspending.JsonDownloader;
import org.junit.Test;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

public class JsonDownloaderTest
{
	@Test public void testGetPartialResults() throws JsonProcessingException, IOException
	{
		JsonDownloader.ResultsReader in = new JsonDownloader.ResultsReader("2013");
		JsonNode node;
		while((node=in.read())!=null ){}
		//fail("not yet finished writing this test");
	}

	@Test public void testGetDatasetNames() throws IOException
	{
		Collection<String> names = JsonDownloader.getDatasetNames();
		assertTrue(names.size()>300);
		assertTrue(names.contains("berlin_de"));
	}

    /**@Test public void testPuzzleTogether() throws FileNotFoundException,IOException
    {
        File testfile1 = new File("json/parts/testfile1");
        try (PrintWriter out = new PrintWriter(testfile1))
        {
            out.println("\"results\": [");
            out.print("{\nhallo\n}");
        }

        File testfile2 = new File("json/parts/testfile2");
        try (PrintWriter out = new PrintWriter(testfile2))
        {
            out.println("\"results\": [");
            out.print("{\nwelt\n}");
        }

        new JsonDownloaderProxy().callPuzzleTogether();

        String input= "";
        try(BufferedReader in = new BufferedReader(new FileReader(new File("json/model"))))
        {
            while((input += in.readLine())!=null){}
        }
        int result = input.compareTo(
                "\"results\": [\n" +
                "{\n" +
                "hallo\n" +
                "}," +
                "\"results\": [\n" +
                "{\n" +
                "welt\n" +
                "},");
        testfile1.delete();
        testfile2.delete();
        assertTrue(result == 0);

    }*/

    public class JsonDownloaderProxy extends JsonDownloader
    {
        public void callPuzzleTogether() throws IOException
        {
            puzzleTogether();
        }
    }
}