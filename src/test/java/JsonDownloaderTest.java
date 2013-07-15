import java.io.IOException;
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
	}
}