package org.aksw.linkedspending;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogManager;
import lombok.extern.java.Log;
import org.aksw.linkedspending.tools.PropertiesLoader;
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
import de.konradhoeffner.commons.MemoryBenchmark;

import static org.aksw.linkedspending.HttpConnectionUtil.*;

/** Downloads entry files from openspending.org. Provides the input for and thus has to be run before Main.java.
 * Datasets are processed in paralllel. Each dataset with more than {@value #pageSize} entries is split into parts with that many entries. **/
@NonNullByDefault
@Log
@SuppressWarnings("serial")
public class JsonDownloader implements Runnable
{
    /** properties */
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");

    static boolean TEST_MODE_ONLY_BERLIN = false;

    static boolean currentlyRunning = true;
    static boolean completeRun = true;
    static String toBeDownloaded;

	static final int MAX_THREADS = 10;
//	static boolean USE_PAGE_SIZE=false;
	// at the moment the pagesize is constant but a possible improvement is dynamic one, starting out high and turning it down when there are errors
	static final int INITIAL_PAGE_SIZE = 100;
	static final int MIN_PAGE_SIZE = 100;
	static final int pageSize = INITIAL_PAGE_SIZE;
	static File folder = new File("json");
	static File rootPartsFolder = new File("json/parts");
	static File modelFolder = new File("json/model");
	static final File DATASETS_CACHED = new File("cache/datasets.json");
	static final File emptyDatasetFile = new File("cache/emptydatasets.ser");
	static {if(!folder.exists()) {folder.mkdir();}}
	static {if(!rootPartsFolder.exists()) {rootPartsFolder.mkdir();}}
	@SuppressWarnings("null") static final Set<String> emptyDatasets = Collections.synchronizedSet(new HashSet<String>());
	static MemoryBenchmark memoryBenchmark = new MemoryBenchmark();

    public static boolean downloadStopped = false;             //testing purposes

	private static final long	TERMINATION_WAIT_DAYS	= 2;

    public static void setToBeDownloaded(String setTo) {toBeDownloaded = setTo;}

    public static void setCurrentlyRunning(boolean setTo) {currentlyRunning=setTo;}

    public static void setCompleteRun(boolean setTo) {completeRun = setTo;}

	public static SortedSet<String> getSavedDatasetNames()
	{
		SortedSet<String> names = new TreeSet<>();
		for(File f: folder.listFiles())
		{
			if(f.isFile()) {names.add(f.getName());}
		}
		return names;
	}

	static protected SortedSet<String> datasetNames = new TreeSet<>();

	public static synchronized SortedSet<String> getDatasetNames() throws IOException
	{
		if(!datasetNames.isEmpty()) return datasetNames;

		JsonNode datasets;
		if(DATASETS_CACHED.exists())
		{
			datasets = Main.m.readTree(DATASETS_CACHED);
		}
		else
		{
			//			System.out.println(new BufferedReader(new InputStreamReader(new URL(Main.DATASETS).openStream())).readLine()); // for manual error detection
			datasets = Main.m.readTree(new URL(PROPERTIES.getProperty("urlDatasets")));
			Main.m.writeTree(new JsonFactory().createGenerator(DATASETS_CACHED, JsonEncoding.UTF8), datasets);
		}
		ArrayNode datasetArray = (ArrayNode)datasets.get("datasets");
		log.info(datasetArray.size()+" datasets available. "+emptyDatasets.size()+" marked as empty, "+(datasetArray.size()-emptyDatasets.size())+" remaining.");
		for(int i=0;i<datasetArray.size();i++)
		{
			JsonNode dataSetJson = datasetArray.get(i);
			datasetNames.add(dataSetJson.get("name").textValue());
		}
		return datasetNames;
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
				try{Thread.sleep(5000);} catch (InterruptedException e)	{log.warning("interrupted thread monitor");}				
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
	static class DownloadCallable implements Callable<Boolean>
	{
		final String datasetName;
		//		final URL entries;
		final int nr;
		//		int pageSize;

		DownloadCallable(String datasetName,int nr) throws MalformedURLException
		{
			this.datasetName = datasetName;
			this.nr=nr;
			//			this.pageSize=pageSize;
			//			entries = new URL("http://openspending.org/"+datasetName+"/entries.json?pagesize="+PAGE_SIZE);
		}

		@Override public @Nullable Boolean call() throws IOException, InterruptedException
		{
			Path path = Paths.get(folder.getPath(),datasetName);
			File file = path.toFile();
			File partsFolder = new File(folder.toString()+"/parts/"+pageSize+"/"+datasetName);			
			File finalPart = new File(partsFolder.toString()+"/"+datasetName+".final");			
			//			Path partsPath = Paths.get(partsFolder.getPath(),datasetName);			
			if(file.exists())
			{
				if(file.length()==0)
				{
					log.severe(file+" exists but is empty, deleting and recreating.");
					file.delete();
				}
				else
				{
					log.finer(nr+" File "+path+" already exists, skipping download.");
					return false;
				}
			}			
			if(partsFolder.exists())
			{
				if(finalPart.exists())
				{
					log.fine(nr+" dataset exists in parts, skipping download..");					
					return false;
				}
				log.fine(nr+" dataset exists in parts but is incomplete, continuing...");
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
				return false;				
			}
			log.info(nr+" Starting download of "+datasetName+", "+nrEntries+" entries.");
			int nrOfPages = (int)(Math.ceil((double)nrEntries/pageSize));

			if(nrOfPages>1)
			{
				partsFolder.mkdirs();
			}
			for(int page=1;page<=nrOfPages;page++)
			{				
				File f = nrOfPages == 1 ? path.toFile(): new File(partsFolder.toString()+"/"+datasetName+"."+(page==nrOfPages?"final":page));
				if(f.exists()) {continue;}
				if(nrOfPages>1) log.fine(nr+" page "+page+"/"+nrOfPages);				
				URL entries = new URL("https://openspending.org/"+datasetName+"/entries.json?pagesize="+pageSize+"&page="+page);
				//				System.out.println(entries);

				try
				{
					HttpURLConnection connection = getConnection(entries);
				}
				catch (HttpTimeoutException | HttpUnavailableException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				ReadableByteChannel rbc = Channels.newChannel(entries.openStream());
				try(FileOutputStream fos = new FileOutputStream(f))
				{fos.getChannel().transferFrom(rbc, 0, Integer.MAX_VALUE);}
				// ideally, memory should be measured during the transfer but thats not easily possible except
				// by creating another thread which is overkill. Because it is multithreaded anyways I hope this value isn't too far from the truth.
				memoryBenchmark.updateAndGetMaxMemoryBytes();
			}
			// TODO: sometimes at the end "]}" is missing, add it in this case
			// manually solvable in terminal with cat /tmp/problems  | xargs -I  @  sh -c "echo ']}' >> '@'"
			// where /tmp/problems is the file containing the list of files with the error 
			log.info(nr+" Finished download of "+datasetName+".");			
			return true;
		}		
	}

	/** downloads a set of datasets. datasets over a certain size are downloaded in parts. Returns true if stopped by Scheduler */
	static boolean downloadIfNotExisting(Collection<String> datasets) throws IOException, InterruptedException, ExecutionException
	{
		int successCount = 0;
		ThreadPoolExecutor service = (ThreadPoolExecutor)Executors.newFixedThreadPool(MAX_THREADS);

		List<Future<Boolean>> futures = new LinkedList<>();
		int i=0;		
		for(String dataset: datasets)
		{
			{
                futures.add(service.submit(new DownloadCallable(dataset,i++)));
                if(!currentlyRunning)             //added to make Downloader stoppable
                {
                    service.shutdown();
                    service.awaitTermination(TERMINATION_WAIT_DAYS, TimeUnit.DAYS);
                    downloadStopped = true;
                    return true;
                }
            }
		}
		ThreadMonitor monitor = new ThreadMonitor(service);
		monitor.start();
		for(Future<Boolean> future : futures)
		{
			try{if(future.get()) {successCount++;}}
			catch(ExecutionException e) {e.printStackTrace();}
		}
		log.info(successCount+" datasets newly created.");
		service.shutdown();
		service.awaitTermination(TERMINATION_WAIT_DAYS, TimeUnit.DAYS);
		monitor.stopMonitoring();
        return false;
	}

	enum Position {TOP,MID,BOTTOM}; 

	/** reconstructs full dataset files out of parts. if you find a better name feel free to change it :-) */
	protected static void puzzleTogether() throws IOException
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
				if(targetFile.length()==0) {throw new RuntimeException(targetFile+" is existing but empty.");}	
				log.finer(targetFile+" already exists. Skipping.");
				continue;
			}
			try(PrintWriter out = new PrintWriter(targetFile))
			{
				int partNr=0;
				File[] parts = datasetToFolder.get(dataset).listFiles();				
				for(File f: parts)
				{
					if(f.length()==0) {throw new RuntimeException(f+" is existing but empty.");}

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

    protected static void downloadSpecific(String datasetName) throws IOException, InterruptedException, ExecutionException
    {
        datasetNames = new TreeSet<>(Collections.singleton(datasetName));
        downloadIfNotExisting(datasetNames);
        try(ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(emptyDatasetFile)))
        {
            out.writeObject(emptyDatasets);
        }
    }

	/** downloads all new datasets which are not marked as empty from a run before. datasets over a certain size are downloaded in parts. */
	protected static void downloadAll() throws JsonProcessingException, IOException, InterruptedException, ExecutionException
	{
        downloadStopped = false;
		if(TEST_MODE_ONLY_BERLIN) {datasetNames=new TreeSet<>(Collections.singleton("berlin_de"));}
		else
		{
			if(emptyDatasetFile.exists())
			{
				try(ObjectInputStream in = new ObjectInputStream(new FileInputStream(emptyDatasetFile)))
				{
					emptyDatasets.addAll((Set<String>) in.readObject());
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
		}
		downloadIfNotExisting(datasetNames);
		try(ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(emptyDatasetFile)))
		{
			out.writeObject(emptyDatasets);
		}	
	}

    @Override
    public void run() /*throws JsonProcessingException, IOException, InterruptedException, ExecutionException*/
    {
        long startTime = System.currentTimeMillis();
        System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
        try{LogManager.getLogManager().readConfiguration();log.setLevel(Level.FINER);} catch ( Exception e ) { e.printStackTrace();}
        try
        {
            if(completeRun) {downloadAll();}
            else {downloadSpecific(toBeDownloaded);}
            puzzleTogether();
        }
        catch (Exception e){/*TODO: Exception Handling*/}
        log.info("Processing time: "+(System.currentTimeMillis()-startTime)/1000+" seconds. Maximum memory usage of "+memoryBenchmark.updateAndGetMaxMemoryBytes()/1000000+" MB.");
        System.exit(0); // circumvent non-close bug of ObjectMapper.readTree
    }
	/** Download all new datasets as json. */
	/*public static void main(String[] args) throws JsonProcessingException, IOException, InterruptedException, ExecutionException
	{
		long startTime = System.currentTimeMillis();
		System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
		try{LogManager.getLogManager().readConfiguration();log.setLevel(Level.FINER);} catch ( Exception e ) { e.printStackTrace();}
		//downloadAll();
        //puzzleTogether();
        Scheduler.runDownloader();
        Scheduler.stopDownloader();
		log.info("Processing time: "+(System.currentTimeMillis()-startTime)/1000+" seconds. Maximum memory usage of "+memoryBenchmark.updateAndGetMaxMemoryBytes()/1000000+" MB.");
		System.exit(0); // circumvent non-close bug of ObjectMapper.readTree
	}*/

}
