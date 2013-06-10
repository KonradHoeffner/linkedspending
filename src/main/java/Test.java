import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import lombok.extern.java.Log;
import com.fasterxml.jackson.core.JsonProcessingException;

@Log
public class Test
{
	public static void main(String[] args) throws JsonProcessingException, MalformedURLException, IOException
	{
//		URL url = new URL("http://openspending.org/sala/entries.json");
		URL url = new URL("http://openspending.org/api/2/search?dataset=sala&format=json");
		
		ReadableByteChannel rbc = Channels.newChannel(url.openStream());
		FileOutputStream fos = new FileOutputStream("json/sala.json");
		fos.getChannel().transferFrom(rbc, 0, Integer.MAX_VALUE);
	}

}