import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream.GetField;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogManager;
import lombok.AllArgsConstructor;
import lombok.extern.java.Log;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import com.fasterxml.jackson.core.JsonEncoding;
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
	static final int PAGE_SIZE = 50000;
	static File folder = new File("json");
	static File rootPartsFolder = new File("json/parts");
	static final File DATASETS_CACHED = new File("cache/datasets.json");
	static final File emptyDatasetFile = new File("cache/emptydatasets.ser");
	static {if(!folder.exists()) {folder.mkdir();}}
	static {if(!rootPartsFolder.exists()) {rootPartsFolder.mkdir();}}
	static Set<String> emptyDatasets = Collections.synchronizedSet(new HashSet<String>());

	public static SortedSet<String> getSavedDatasetNames()
	{
		SortedSet<String> names = new TreeSet<>();
		for(File f: folder.listFiles())
		{
			if(f.isFile()) {names.add(f.getName());}
		}
		return names;
	}

	static SortedSet<String> datasetNames = null;
	public static SortedSet<String> getDatasetNames() throws JsonProcessingException, IOException
	{
		if(datasetNames!=null) return datasetNames;

		SortedSet<String> names = new TreeSet<>();
		JsonNode datasets;
		if(DATASETS_CACHED.exists())
		{
			datasets = Main.m.readTree(DATASETS_CACHED);
		}
		else
		{
			datasets = Main.m.readTree(new URL(Main.DATASETS));
			Main.m.writeTree(new JsonFactory().createGenerator(DATASETS_CACHED, JsonEncoding.UTF8), datasets);
		}
		ArrayNode datasetArray = (ArrayNode)datasets.get("datasets");
		log.info(datasetArray.size()+" datasets available. "+emptyDatasets.size()+" marked as empty, "+(datasetArray.size()-emptyDatasets.size())+" remaining.");
		for(int i=0;i<datasetArray.size();i++)
		{
			JsonNode dataSetJson = datasetArray.get(i);
			names.add(dataSetJson.get("name").textValue());
		}
		datasetNames=names;
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

	static class ThreadMonitor extends Thread
	{
		//		final Collection<DownloadCallable> callables;		
		//		public ThreadMonitor(Collection<DownloadCallable> callables)	{this.callables=callables;}
		final ThreadPoolExecutor executor;		
		public ThreadMonitor(ThreadPoolExecutor executor)	{this.executor=executor;}
		boolean running = true;

		public void stopMonitoring() {running=false;}

		@Override public void run()
		{
			Set<Integer> nrs = new TreeSet<Integer>();
			while(running)
			{
				try{Thread.sleep(5000);} catch (InterruptedException e)	{throw new RuntimeException();}				
				//				synchronized(callables)
				//				{
				//					for(DownloadCallable callable: callables)
				//					{
				//						nrs.add(callable.nr);
				//					}
				//					System.out.println("Active threads:"+nrs);
				//				}
				log.finer(executor.getCompletedTaskCount()+" completed, "+executor.getActiveCount()+" active.");
			}
			log.fine("stopped monitoring");
		}
	}

	/** If the dataset has no more than PAGE_SIZE results, it gets saved to json/datasetName, else it gets split into parts
	 * in the folder json/parts/pagesize/datasetname with filenames datasetname.0, datasetname.1, ... , datasetname.final **/
	static class DownloadCallable implements Callable<Void>
	{
		final String datasetName;
		//		final URL entries;
		final int nr;

		DownloadCallable(String datasetName,int nr) throws MalformedURLException
		{
			this.datasetName = datasetName;
			this.nr=nr;
			//			entries = new URL("http://openspending.org/"+datasetName+"/entries.json?pagesize="+PAGE_SIZE);
		}

		@Override public @Nullable Void call() throws IOException
		{			
			Path path = Paths.get(folder.getPath(),datasetName);
			File partsFolder = new File(folder.toString()+"/parts/"+PAGE_SIZE+"/"+datasetName);			
			File finalPart = new File(partsFolder.toString()+"/"+datasetName+".final");			
			//			Path partsPath = Paths.get(partsFolder.getPath(),datasetName);
			if(path.toFile().exists())
			{
				log.finer(nr+" File "+path+" already exists, skipping download.");
				return null;
			}
			System.out.println("bla");
			System.out.println(partsFolder);
			if(partsFolder.exists())
			{
				if(finalPart.exists())
				{
					log.fine(nr+" dataset exists in parts, skipping download..");					
					return null;
				}
				log.fine(nr+"dataset exists in parts but is incomplete, continuing...");
			}
			log.fine(nr+" Fetching number of entries for dataset "+datasetName);		
			int nrEntries = Main.nrEntries(datasetName);
			if(nrEntries==0)
			{
				log.fine(nr+" No entries for dataset "+datasetName+" skipping download.");
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
			log.info(nr+" Starting download of "+datasetName+", "+nrEntries+" entries.");
			int nrOfPages = (int)(Math.ceil((double)nrEntries/PAGE_SIZE));

			if(nrOfPages>1)
			{
				partsFolder.mkdirs();
			}
			for(int page=1;page<=nrOfPages;page++)
			{				
				File f = nrOfPages == 1 ? path.toFile(): new File(partsFolder.toString()+"/"+datasetName+"."+(page==nrOfPages?"final":page));
				if(f.exists()) {continue;}
				if(nrOfPages>1) log.fine(nr+" page "+page+"/"+nrOfPages);				
				URL entries = new URL("http://openspending.org/"+datasetName+"/entries.json?pagesize="+PAGE_SIZE+"&page="+page);
				System.out.println(entries);
				ReadableByteChannel rbc = Channels.newChannel(entries.openStream());
				try(FileOutputStream fos = new FileOutputStream(f))
				{fos.getChannel().transferFrom(rbc, 0, Integer.MAX_VALUE);}				
			}
			// TODO: sometimes at the end "]}" is missing, add it in this case
			// manually solvable in terminal with cat /tmp/problems  | xargs -I  @  sh -c "echo ']}' >> '@'"
			// where /tmp/problems is the file containing the list of files with the error 
			log.info(nr+" Finished download of "+datasetName+".");			
			return null;
		}		
	}

	static void downloadIfNotExisting(Collection<String> datasets) throws IOException, InterruptedException, ExecutionException
	{
		ThreadPoolExecutor service = (ThreadPoolExecutor)Executors.newFixedThreadPool(MAX_THREADS);

		List<Future<Void>> futures = new LinkedList<>();
		int i=0;		
		for(String dataset: datasets)
		{
			{futures.add(service.submit(new DownloadCallable(dataset,i++)));}			
		}
		ThreadMonitor monitor = new ThreadMonitor(service);
		monitor.start();		
		service.shutdown();

		service.awaitTermination(10, TimeUnit.DAYS);
		monitor.stopMonitoring();		
	}

	enum Position {TOP,MID,BOTTOM}; 

	static void puzzleTogether() throws IOException
	{
		Set<String> inParts = new HashSet<>();
		Map<String,File> datasetToFolder = new HashMap<>();
		for(File pageSizeFolder : rootPartsFolder.listFiles())
		{			
			for(File folder : pageSizeFolder.listFiles())
			{				
				datasetToFolder.put(folder.getName(),folder);
			}
		}
		//		Set<String> unpuzzled = datasetToFolder.keySet();		
		//		unpuzzled.removeAll(getSavedDatasetNames());		
		for(String dataset:datasetToFolder.keySet())
		{			
			File targetFile = new File(folder.getPath()+"/"+dataset);
			if(targetFile.exists())
			{
				log.info(targetFile+" already exists. Skipping.");
				continue;
			}
			try(PrintWriter out = new PrintWriter(targetFile))
			{
				int partNr=0;
				File[] parts = datasetToFolder.get(dataset).listFiles();				
				for(File f: parts)
				{
					if(f.exists()) {log.info(f+" already exists, skipping.");continue;}
					System.out.println(f);
					Position pos = Position.TOP;
					try(BufferedReader in = new BufferedReader(new FileReader(f)))
					{
						String line;
						while((line= in.readLine())!=null)
						{
							switch(pos)
							{	
								case TOP:		if(partNr==0) out.println(line);if(line.contains("\"results\": [")) pos=Position.MID;break;
								case MID:		out.println(line);if(line.equals("}")) pos=Position.BOTTOM;break;							
								case BOTTOM:	if(partNr==parts.length-1) out.println(line);break;
							}
						}					
					}
					if(partNr!=parts.length-1) out.print(",");
					partNr++;					
				}
			}
		}		
	}

	static void downloadAll() throws JsonProcessingException, IOException, InterruptedException, ExecutionException
	{		
		if(emptyDatasetFile.exists())
		{
			try(ObjectInputStream in = new ObjectInputStream(new FileInputStream(emptyDatasetFile)))
			{
				emptyDatasets = (Set<String>) in.readObject();
			}
			catch (Exception e) {log.warning("Error reading empty datasets file");}
		}
		Collection<String> datasetNames = getDatasetNames();

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

	public static void main(String[] args) throws JsonProcessingException, IOException, InterruptedException, ExecutionException
	{
		System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
		try{LogManager.getLogManager().readConfiguration();log.setLevel(Level.FINER);} catch ( Exception e ) { e.printStackTrace();}
		downloadAll();
		puzzleTogether();
		System.exit(0); // circumvent non-close bug of ObjectMapper.readTree
	}

}