import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream.GetField;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.LogManager;
import lombok.AllArgsConstructor;
import lombok.extern.java.Log;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.node.ArrayNode;

/** Used for downloading entry files from openspending.org and retrieving them as a string. Multithreaded. 
 * */
@NonNullByDefault
@Log
public class JsonDownloader
{
	static final int MAX_THREADS = 10;
	static File folder = new File("json");
	static final File emptyDatasetFile = new File("cache/emptydatasets.ser");
	static {if(!folder.exists()) {folder.mkdir();}}
	static Set<String> emptyDatasets = Collections.synchronizedSet(new HashSet<String>());

	public static SortedSet<String> getDatasetNames()
	{
		SortedSet<String> names = new TreeSet<>();
		for(File f: folder.listFiles())
		{
			names.add(f.getName());
		}
		return names;
	}

	static File getFile(String datasetName) {return Paths.get(folder.getPath(),datasetName).toFile();}

	public static @NonNull ArrayNode getResults(String datasetName) throws JsonProcessingException, IOException
	{
		return (ArrayNode)Main.m.readTree(getFile(datasetName)).get("results");		
	}

	public static class ResultsReader
	{
		final protected JsonParser jp;
		
		public ResultsReader(String datasetName) throws JsonParseException, IOException
		{
			JsonFactory f = new MappingJsonFactory();
			jp = f.createParser(getFile(datasetName));
			JsonToken current = jp.nextToken();
			if (current != JsonToken.START_OBJECT) {
				System.out.println();
				throw new IOException("Error with dataset "+datasetName+": root should be object: quiting.");
			}		
			while (!"results".equals(jp.getCurrentName())) {jp.nextToken();}
			if (jp.nextToken() != JsonToken.START_ARRAY)
			{throw new IOException("Error with dataset "+datasetName+": array expected.");}
		}

		@Nullable public JsonNode read() throws JsonParseException, IOException
		{
			if(jp.nextToken() == JsonToken.END_ARRAY) {jp.close();return null;}			  
			JsonNode node = jp.readValueAsTree();
			return node;
		}
	}

	static class DownloadCallable implements Callable<Void>
	{
		final String datasetName;
		final URL entries;
		final int nr;

		DownloadCallable(String datasetName,int nr) throws MalformedURLException
		{
			this.datasetName = datasetName;
			this.nr=nr;
			entries = new URL("http://openspending.org/"+datasetName+"/entries.json?pagesize="+Integer.MAX_VALUE);
		}

		@Override public @Nullable Void call() throws IOException
		{			
			Path path = Paths.get(folder.getPath(),datasetName);
			if(path.toFile().exists())
			{
				log.finer(nr+" File "+path+" already exists, skipping download.");
				return null;
			}
			log.fine(nr+" Fetching number of entries for dataset "+datasetName);		
			if(Main.nrEntries(datasetName)==0)
			{
				log.finer(nr+" No entries for dataset "+datasetName+" skipping download.");
				emptyDatasets.add(datasetName);
				synchronized(emptyDatasets)
				{
					try(ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(emptyDatasetFile)))
					{
						log.fine(nr+" serializing "+emptyDatasets.size()+" entries to file");
						out.writeObject(emptyDatasets);
					}
				}
				// save as empty file to make it faster? but then it slows down normal use
				return null;				
			}
			log.info(nr+" Starting download of "+datasetName+".");
			{
				ReadableByteChannel rbc = Channels.newChannel(entries.openStream());
				try(FileOutputStream fos = new FileOutputStream(path.toFile()))
				{fos.getChannel().transferFrom(rbc, 0, Integer.MAX_VALUE);}
				// TODO: sometimes at the end "]}" is missing, add it in this case
				// manually solvable in terminal with cat /tmp/problems  | xargs -I  @  sh -c "echo ']}' >> '@'"
				// where /tmp/problems is the file containing the list of files with the error 
			}
			log.info(nr+" Finished download of "+datasetName+".");			
			return null;
		}		
	}

	static void downloadIfNotExisting(Collection<String> datasets) throws IOException, InterruptedException, ExecutionException
	{
		ExecutorService service = Executors.newFixedThreadPool(MAX_THREADS);

		List<Future<Void>> futures = new LinkedList<>();
		int i=0;		
		for(String dataset: datasets)
		{
			{futures.add(service.submit(new DownloadCallable(dataset,i++)));}
		}
		for(Future<Void> future: futures)
		{
			//			future.get();
			//			System.out.println("***************finished a future***************");
		}
	}

	public static void main(String[] args) throws JsonProcessingException, IOException, InterruptedException, ExecutionException
	{
		System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
		try{LogManager.getLogManager().readConfiguration();log.setLevel(Level.FINER);} catch ( Exception e ) { e.printStackTrace();}

		if(emptyDatasetFile.exists())
		{
			try(ObjectInputStream in = new ObjectInputStream(new FileInputStream(emptyDatasetFile)))
			{
				emptyDatasets = (Set<String>) in.readObject();
			}
			catch (Exception e) {log.warning("Error reading empty datasets file");}
		}
		Collection<String> datasetNames = new HashSet<>();

		JsonNode datasets = Main.m.readTree(new URL(Main.DATASETS));			
		ArrayNode datasetArray = (ArrayNode)datasets.get("datasets");
		for(int i=0;i<datasetArray.size();i++)
		{
			JsonNode dataSetJson = datasetArray.get(i);
			datasetNames.add(dataSetJson.get("name").textValue());
		}
		datasetNames.removeAll(emptyDatasets);
		{
			List datasetNamesShuffled = new ArrayList<>(datasetNames);
			Collections.shuffle(datasetNamesShuffled);
			datasetNames=datasetNamesShuffled;
		}
		downloadIfNotExisting(datasetNames);

		try(ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(emptyDatasetFile)))
		{
			out.writeObject(emptyDatasets);
		}
	}

}