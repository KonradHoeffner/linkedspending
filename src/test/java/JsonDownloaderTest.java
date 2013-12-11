import static org.junit.Assert.*;
import java.io.IOException;
import java.util.Collection;
import org.aksw.linkedspending.JsonDownloader;
import org.junit.Test;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

public class JsonDownloaderTest
{
	@Test public void testGetPartialResults() throws JsonProcessingException, IOException
	{
		JsonDownloader.ResultsReader in = new JsonDownloader.ResultsReader("eu-commission-fts");
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

}