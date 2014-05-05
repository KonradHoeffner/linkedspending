package org.aksw.linkedspending;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import de.konradhoeffner.commons.MemoryBenchmark;
import lombok.extern.java.Log;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import org.aksw.linkedspending.tools.EventNotification;
import org.aksw.linkedspending.tools.EventNotificationContainer;
import org.aksw.linkedspending.tools.PropertiesLoader;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.LogManager;

import static org.aksw.linkedspending.HttpConnectionUtil.*;

/** Downloads entry files from openspending.org. Provides the input for and thus has to be run before Converter.
 * Datasets are processed in paralllel. Each dataset with more than {@value #pageSize} entries is split into parts with that many entries. **/
@NonNullByDefault
@Log
@SuppressWarnings("serial")
public class JsonDownloader implements Runnable
{

    //todo differentiated exception handling needed in hole class

    // public static boolean finished = false;

    /** helps to merge JSON-parts by representing a relative position in a given parts-file*/
    enum Position {TOP,MID,BOTTOM};
    /** external properties to be used in Project */
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");
    /**testmode that makes Downloader only download one file from openspending:"berlin_de"*/
    static boolean TEST_MODE_ONLY_BERLIN = false;
    /**sets downloader stopable*/
    private static boolean stopRequested = false;
    /**pauses downloader until set to false again*/
    private static boolean pauseRequested =false;
    /**makes downloader load all files from openspending; opposite concrete file in field toBeDownloaded*/
    private static boolean completeRun = true;
    /**field with one(not shure if several possible too) specific file to be downloaded from openspending; used, when completeRun=false*/
    private static String toBeDownloaded;
    /**maximum number of threads used by downloader*/
    static final int MAX_THREADS = 10;
    /**object to handle event notifications in hole linkedspending system*/
    private static EventNotificationContainer eventContainer = new EventNotificationContainer();

    //todo form KHoeffner:"at the moment the pagesize is constant but a possible improvement is dynamic one, starting out high and turning it down when there are errors"

    /**???not used anyway*/
    static boolean USE_PAGE_SIZE=false;
    /**the initial page size
     * @see #pageSize*/
    static final int INITIAL_PAGE_SIZE = 100;
    /**???not used anyway*/
    static final int MIN_PAGE_SIZE = 100;
    /**the maximum number of JSON-objects in the JSON-array of a downloaded file in the parts folder<br>
     * explanation: The downloader loads JSON-files from openspending. The JSON-files are stored in .../json.
     * If the number of entries is bigger than pagesize, the file is split into several parts and stored in the .../json/parts/"pagesize"/"datasetname" folder.
     * Else the file is stored completely in the .../json/"datasetname" file.*/
    static final int pageSize = INITIAL_PAGE_SIZE;
    /**the name of the folder, where the downloaded JSON-files are stored*/
    static File folder = new File(PROPERTIES.getProperty("pathJson"));
    /**name of the root-folder, where the downloaded and splitted JSON-files are stored
     * @see #pageSize "pageSize" for more details*/
    static File rootPartsFolder = new File(PROPERTIES.getProperty("pathParts"));
    /**???not used anyway*/
    static File modelFolder = new File("json/model");
    static final File CACHE = new File("cache");
    /**path for file that gives metainformation about already downloaded(or downloadable) JSON-files available at openspending e.g. number of datasets in german <br>
     * and also metainformation about concrete donwloaded JSON-files e.g.the url of the file or last_modified */
    static final File DATASETS_CACHED = new File("cache/datasets.json");
    /**file that stores reference to all empty datasets*/
    static final File emptyDatasetFile = new File("cache/emptydatasets.ser");
    /**set for the names of already locally saved JSON-files known to the downloader*/
    static protected SortedSet<String> datasetNames = new TreeSet<>();

    static {if(!CACHE.exists()) {CACHE.mkdir();}}
    static {if(!folder.exists()) {folder.mkdir();}}
    static {if(!rootPartsFolder.exists()) {rootPartsFolder.mkdir();}}

    /**represents all the empty JSON-files in a set; highly interacts with: emptyDatasetFile<br>
     * is used for example to remove empty datasets from downloading-process
     * @see #emptyDatasetFile */
    @SuppressWarnings("null") static final Set<String> emptyDatasets = Collections.synchronizedSet(new HashSet<String>());
    /**used to provide one statistical value: "the maximum memory used by jvm while downloading*/
    static MemoryBenchmark memoryBenchmark = new MemoryBenchmark();
    /**used to convert from JSON-file to Java-object and vice versa*/
    static ObjectMapper m = new ObjectMapper();
    /**whether the cache is used or not*/
    static final boolean USE_CACHE = false;// Boolean.parseBoolean(PROPERTIES.getProperty("useCache", "true"));

    //todo following comment
    /**???is a cache if USE_CACHE=true, otherwise null*/
    static final Cache cache = USE_CACHE? CacheManager.getInstance().getCache("openspending-json"):null;

    //todo accessing cache causes NullPointerException (in readJSONString())

    /**testing purposes*/
    public static boolean downloadStopped = false;
    /**The maximum days the downloader is waiting until shutdown.
     * Once a stopRequested=true signal is send to downloader it blocks and tries to finish its last tasks before shutting down.*/
    private static final long    TERMINATION_WAIT_DAYS    = 2;

    /**
     * gets event container to deal with events
     * @return the event container
     */
    public static EventNotificationContainer getEventContainer() {return eventContainer;}

    /**
     * sets a JSON-file to be downloaded from openspending
     * @param setTo the filename of the JSON-file
     * @see #toBeDownloaded
     */
    public static void setToBeDownloaded(String setTo) {toBeDownloaded = setTo;}

    /**sets the property stopRequested wich makes Downloader stopable,
     * used by scheduler to stop JsonDownloader
     * @param setTo true makes downloader stopable*/
    public static void setStopRequested(boolean setTo) {stopRequested=setTo;}

    /**
     * sets whether all files are to be downloaded from openspending
     * @param setTo true if all files are to be downloaded
     * @see #completeRun
     */
    public static void setCompleteRun(boolean setTo) {completeRun = setTo;}

    /**
     * sets whether the downloader should stop, even before having finished
     * @param setTo true if downloader shall stop
     * @see #pauseRequested
     */
    public static void setPauseRequested(boolean setTo) {pauseRequested = setTo;}

    /**
     * returns a JSON-string from the given url
     * @param url the url where the JSON-string is located
     * @return a string containing a JSON-object
     * @throws IOException
     */
    public static String readJSONString(URL url) throws IOException {
        return readJSONString(url, false, USE_CACHE);
    }

    /**
     * returns a JSON-string from the given url
     * @param url the url where the JSON-string is located
     * @param detailedLogging true for better logging
     * @return a string containing a JSON-object
     * @throws IOException
     */
    public static String readJSONString(URL url,boolean detailedLogging) throws IOException {
        return readJSONString(url, detailedLogging, USE_CACHE);
    }

    /**
     * reads a JSON-string from openspending and returns it
     * @param url the url for the string
     * @param detailedLogging true for better logging
     * @param USE_CACHE
     * @return a JSON-string
     * @throws IOException
     */
    public static String readJSONString(URL url,boolean detailedLogging,boolean USE_CACHE) throws IOException
    {
        //        System.out.println(cache.getKeys());
        if(USE_CACHE)
        {
            Element e = cache.get(url.toString());
            if(e!=null) {/*System.out.println("cache hit for "+url.toString());*/return (String)e.getObjectValue();}
        }
        if(detailedLogging) {log.fine("cache miss for "+url.toString());}

        // SWP 14 team: here is a start for the response code handling which you should get to work, I discontinued it because the connection
        // may be a non-httpurlconnection (if the url relates to a file) so maybe there should be two readJsonString methods, one for a file and one for an http url
        // or maybe it should be split into two methods where this one only gets a string as an input and the error handling for connections should be somewhere else
        // of course there shouldn't be System.out.println() statements, they are just placeholders.
        // error handling isnt even that critical here but needs to be in any case in the JSON downloader for the big parts
        //        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        //        connection.connect();
        //        int response = connection.getResponseCode();
        //        switch(response)
        //        {
        //            case HttpURLConnection.HTTP_OK: System.out.println("OK"); // fine, continue
        //            case HttpURLConnection.HTTP_GATEWAY_TIMEOUT: System.out.println("gateway timeout"); // retry
        //            case HttpURLConnection.HTTP_UNAVAILABLE: System.out.println("unavailable"); // abort
        //            default: log.error("unhandled http response code "+response+". Aborting download of dataset."); // abort
        //        }
        //        try(Scanner undelimited = new Scanner(connection.getInputStream(), "UTF-8"))
        try(Scanner undelimited = new Scanner(url.openStream(), "UTF-8"))
        {
            try(Scanner scanner = undelimited.useDelimiter("\\A"))
            {
                String datasetsJsonString = scanner.next();
                char firstChar = datasetsJsonString.charAt(0);
                if(!(firstChar=='{'||firstChar=='[')) {throw new IOException("JSON String for URL "+url+" seems to be invalid.");}
                if(USE_CACHE) {cache.put(new Element(url.toString(), datasetsJsonString));}
                //IfAbsent
                return datasetsJsonString;
            }
        }
    }

    /**
     * reads a JSON-string from an url and converts it into a JSON-object
     * @param url the url where the JSON-string is located
     * @param detailedLogging true for better logging
     * @return a JSON-object
     * @throws JsonProcessingException
     * @throws IOException
     */
    public static JsonNode readJSON(URL url,boolean detailedLogging) throws JsonProcessingException, IOException
    {
        String content = readJSONString(url,detailedLogging);
        if(detailedLogging) {log.fine("finished loading text, creating json object from text");}
        return m.readTree(content);
        //        try {return new JsonNode(readJSONString(url));}
        //        catch(JSONException e) {throw new IOException("Could not create a JSON object from string "+readJSONString(url),e);}
    }

    /**
     * reads a JSON-string from an url and converts it into a JSON-object
     * @param url the url where the JSON-string is located
     * @return a JSON-object
     * @throws IOException
     */
    public static JsonNode readJSON(URL url) throws IOException
    {
        return readJSON(url,false);
    }

    //todo does the cache file get updated once in a while? if not functionality is needed
    /**
     * loads the names of datasets(JSON-files) <br>
     * if #datasetNames already exists, return them<br>
     * if cache-file exists, load datasets from cache-file<br>
     * if cache-file does not exist, load from openspending and write cache-file
     * @return a set containing the names of all JSON-files
     * @throws IOException - if one of many files can't be read from or written to
     */
    public static synchronized SortedSet<String> getDatasetNames() throws IOException
    {
        if(!datasetNames.isEmpty()) return datasetNames;

        JsonNode datasets;

        //load from cache
        if(DATASETS_CACHED.exists())
        {
            datasets = m.readTree(DATASETS_CACHED);
        }
        //load from openspending and write cache
        else
        {
            datasets = m.readTree(new URL(PROPERTIES.getProperty("urlDatasets")));
            m.writeTree(new JsonFactory().createGenerator(DATASETS_CACHED, JsonEncoding.UTF8), datasets);
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

    /**
     * returns a file from the already downloaded datasets
     * @param datasetName the name of the file
     * @return the file to the given dataset
     */
    static File getFile(String datasetName) {return Paths.get(folder.getPath(),datasetName).toFile();}

    //todo what is this method for?
    public static @NonNull ArrayNode getResults(String datasetName) throws JsonProcessingException, IOException
    {
        return (ArrayNode)m.readTree(getFile(datasetName)).get("results");
    }

    //todo All 5 readJSON... methods exist only to retrieve one integer-value from an url???(some side effects are error handling)
    /**
     * Reads a JSON-string from an url of openspending. Converts it into a JSON-object
     * Retrieves only one Integer-value from the field:"results_count_query".
     * @param datasetName the name of a dataset on openspending
     * @return the number of results, that the dataset contains
     * @throws MalformedURLException
     * @throws IOException
     */
    public static int nrEntries(String datasetName) throws MalformedURLException, IOException
    {
        return readJSON(new URL(PROPERTIES.getProperty("urlOpenSpending") + datasetName + "/entries.json?pagesize=0")).get("stats").get("results_count_query").asInt();
    }

    /**
     * Class to create a thread. The threads purpose is to monitor the threadpool, which does the downloading of all JSON-files from openspending.
     */
    static class ThreadMonitor extends Thread
    {
        //        final Collection<DownloadCallable> callables;
        //        public ThreadMonitor(Collection<DownloadCallable> callables)    {this.callables=callables;}
        final ThreadPoolExecutor executor;
        public ThreadMonitor(ThreadPoolExecutor executor)    {this.executor=executor;}
        boolean running = true;

        public void stopMonitoring() {running=false;}

        @Override public void run()
        {
            Set<Integer> nrs = new TreeSet<Integer>();
            while(running)
            {
                try{Thread.sleep(5000);} catch (InterruptedException e)    {log.warning("interrupted thread monitor");}
                //                synchronized(callables)
                //                {
                //                    for(DownloadCallable callable: callables)
                //                    {
                //                        nrs.add(callable.nr);
                //                    }
                //                    System.out.println("Active threads:"+nrs);
                //                }
                log.finer(executor.getCompletedTaskCount()+" completed, "+executor.getActiveCount()+" active.");
            }
            log.fine("stopped monitoring");
        }
    }


    /**Implements the logic for downloading a JSON-file within a thread. Is similar to the use of the Runnable Interface, but its call method can give a return value.<p>
     * If the dataset has no more than PAGE_SIZE results, it gets saved to json/datasetName, else it gets split into parts
     * in the folder json/parts/pagesize/datasetname with filenames datasetname.0, datasetname.1, ... , datasetname.final **/
    static class DownloadCallable implements Callable<Boolean>
    {
        /**name of the dataset to be downloaded*/
        final String datasetName;
        //        final URL entries;
        /**id for the Instance*/
        final int nr;
        //        int pageSize;

        /**
         * normal constructor
         * @param datasetName the name of the dataset to be downloaded
         * @param nr the id for this instance
         * @throws MalformedURLException
         */
        DownloadCallable(String datasetName,int nr) throws MalformedURLException
        {
            this.datasetName = datasetName;
            this.nr=nr;
            //            this.pageSize=pageSize;
            //            entries = new URL("http://openspending.org/"+datasetName+"/entries.json?pagesize="+PAGE_SIZE);
        }

        /**
         * implements the real logic for downloading a file from openspending
         * @return true if dataset was completely downloaded, false otherwise
         * @throws IOException
         * @throws InterruptedException
         */
        @Override public @Nullable Boolean call() throws IOException, InterruptedException
        {
            Path path = Paths.get(folder.getPath(),datasetName);
            File file = path.toFile();
            File partsFolder = new File(folder.toString()+"/parts/"+datasetName);
            File finalPart = new File(partsFolder.toString()+"/"+datasetName+".final");
            //            Path partsPath = Paths.get(partsFolder.getPath(),datasetName);
            log.fine(nr + " Fetching number of entries for dataset " + datasetName);

            //here is where all the readJSON... stuff is exclusively used
            int nrEntries = nrEntries(datasetName);
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

            partsFolder.mkdirs();
            // starts from beginning when final file already exists
            File finalFile = new File(partsFolder.toString() + "/" + datasetName + ".final");
            if(finalFile.exists()) {
                for (File part : partsFolder.listFiles()) {
                    part.delete();
                }
            }
            for(int page=1;page<=nrOfPages;page++)
            {
                File f = new File(partsFolder.toString()+"/"+datasetName+"."+(page==nrOfPages?"final":page));
                if(f.exists()) {continue;}
                log.fine(nr+" page "+page+"/"+nrOfPages);
                URL entries = new URL("https://openspending.org/"+datasetName+"/entries.json?pagesize="+pageSize+"&page="+page);
                //                System.out.println(entries);

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

    /**
     * downloads a set of datasets. datasets over a certain size are downloaded in parts.
     * @param datasets a Collection of all filenames to be downloaded from openspending
     * @return returns true if stopped by Scheduler, false otherwise
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    static boolean downloadIfNotExisting(Collection<String> datasets) throws IOException, InterruptedException, ExecutionException
    {
        int successCount = 0;
        ThreadPoolExecutor service = (ThreadPoolExecutor)Executors.newFixedThreadPool(MAX_THREADS);
        List<Future<Boolean>> futures = new LinkedList<>();
        int i=0;
        //creates a Future for each file that is to be downloaded
        for(String dataset: datasets)
        {
            {
                if(pauseRequested)
                {
                    eventContainer.getEventNotifications().add(new EventNotification(12,1));
                    while(pauseRequested) {}
                    eventContainer.getEventNotifications().add(new EventNotification(13,1));
                }
                futures.add(service.submit(new DownloadCallable(dataset,i++)));
                if(stopRequested)             //added to make Downloader stoppable
                {
                    eventContainer.getEventNotifications().add(new EventNotification(11,1));
                    service.shutdown();
                    service.awaitTermination(TERMINATION_WAIT_DAYS, TimeUnit.DAYS);
                    downloadStopped = true;
                    return true;
                }
            }
        }
        ThreadMonitor monitor = new ThreadMonitor(service);
        monitor.start();
        //here starts the real downloading of all JSON-files(takes a long while)
        for(Future<Boolean> future : futures)
        {
            try{if(future.get()) {successCount++;}}
            catch(ExecutionException e) {e.printStackTrace();}
        }
        //cleaning up
        log.info(successCount+" datasets newly created.");
        service.shutdown();
        service.awaitTermination(TERMINATION_WAIT_DAYS, TimeUnit.DAYS);
        monitor.stopMonitoring();
        return false;
    }


    /**
     * Collects all parted Datasets from a specific File
     * @param foldername the Place where the parted Files are found
     * @return returns a Hashmap with all Files found in the given Folder.
     */
    protected static Map<String, File> getDataFiles(File foldername)
    {
        Map<String,File> datasetToFolder = new HashMap<>();
        for(File folder : foldername.listFiles())
        {
            if(folder.isDirectory()) {
                datasetToFolder.put(folder.getName(), folder);
            }
        }
        return datasetToFolder;
    }

    /** merges all files collected in partData and writes the targetfile to the other already complete ones
     *
     * @param partData A Hashmap with all Datasets that need to be merged
     */

    protected static void mergeJsonParts(Map<String, File> partData)
    {
        //for each folder containing parts
        for (String dataset : partData.keySet()) {
            File targetFile = new File(folder.getPath() + "/" + dataset);
            File mergeFile = new File(folder.getPath() + "/" + dataset + ".tmp");
            if(mergeFile.exists()) {
                mergeFile.delete();
            }

            try (PrintWriter out = new PrintWriter(mergeFile)) {
                int partNr = 0;
                File[] parts = partData.get(dataset).listFiles();
                //for each file in the parts folder
                for (File f : parts) {
                    if (f.length() == 0) {
                        log.severe(f + " is existing but empty.");
                    }
                    Position pos = Position.TOP;
                    try (BufferedReader in = new BufferedReader(new FileReader(f))) {
                        String line;
                        //each line in a parts-file
                        while ((line = in.readLine()) != null) {
                            switch (pos) {
                                case TOP:
                                    if (partNr == 0) out.println(line);
                                    if (line.contains("\"results\": [")) pos = Position.MID;
                                    break;
                                case MID:
                                    out.println(line);
                                    if (line.equals("    }")) pos = Position.BOTTOM;
                                    break;
                                case BOTTOM:
                                    if (partNr == parts.length - 1) out.println(line);
                                    break;
                            }
                        }
                    } catch (IOException e) {
                        log.severe("could not write read parts file for " + dataset + ": " + e.getMessage());
                    }
                    if (partNr != parts.length - 1) out.print(",");
                    partNr++;
                }
            } catch (IOException e) {
                log.severe("could not create merge file for " + dataset + ": "+ e.getMessage());
            }

            if (targetFile.exists()) {
                boolean equals;
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode target = mapper.readTree(new FileInputStream(targetFile));
                    JsonNode merge = mapper.readTree(new FileInputStream(mergeFile));
                    equals = target.equals(merge);
                } catch (Exception e) {
                    log.severe("could not compare files for " + dataset + ": " + e.getMessage());
                    equals = false;
                }
                if (equals) {
                    mergeFile.delete();
                } else {
                    targetFile.delete();
                    mergeFile.renameTo(targetFile);

                }
            } else {
                mergeFile.renameTo(targetFile);
            }
        }
    }

    /**
     * merges part-files
     * @see #mergeJsonParts(java.util.Map)
     */
    protected static void puzzleTogether()
    {
        mergeJsonParts(getDataFiles(rootPartsFolder));
    }

    /**
     * Downloads one specific dataset-file from openspending.
     * Writes the emptyDatasetFile.
     * @param datasetName the name of the dataset to be downloaded
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected static void downloadSpecific(String datasetName) throws IOException, InterruptedException, ExecutionException
    {
        datasetNames = new TreeSet<>(Collections.singleton(datasetName));
        downloadIfNotExisting(datasetNames);
        //try-with-resources(since java 7)
        //todo why is this here?
        try(ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(emptyDatasetFile)))
        {
            out.writeObject(emptyDatasets);
        }
    }

    /**
     * downloads all new datasets which are not marked as empty from a run before. datasets over a certain size are downloaded in parts.
     * @throws JsonProcessingException
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
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
            datasetNames = getDatasetNames();
            datasetNames.removeAll(emptyDatasets);
        }
        downloadIfNotExisting(datasetNames);
        try(ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(emptyDatasetFile)))
        {
            out.writeObject(emptyDatasets);
        }
    }

    @Override
    /**
     * The main method for the Downloader. Starts Downloader.
     */
    public void run() /*throws JsonProcessingException, IOException, InterruptedException, ExecutionException*/
    {
        //finished = false;
        long startTime = System.currentTimeMillis();
        System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
        try{LogManager.getLogManager().readConfiguration();log.setLevel(Level.FINER);} catch ( Exception e ) { e.printStackTrace();}
        try
        {
            if(completeRun)
            {
                eventContainer.getEventNotifications().add(new EventNotification(8,1));
                downloadAll();
                eventContainer.getEventNotifications().add(new EventNotification(1,1,true));
            }
            else
            {
                eventContainer.getEventNotifications().add(new EventNotification(7,1));
                downloadSpecific(toBeDownloaded);
                eventContainer.getEventNotifications().add(new EventNotification(0,1,true));
            }
            puzzleTogether();
        }
        catch (Exception e){e.printStackTrace();}
        //finished = true;
        log.info("Processing time: "+(System.currentTimeMillis()-startTime)/1000+" seconds. Maximum memory usage of "+memoryBenchmark.updateAndGetMaxMemoryBytes()/1000000+" MB.");
        System.exit(0); // circumvent non-close bug of ObjectMapper.readTree
    }

    /** Main method for testing and developing purposes only */
    public static void main(String[] args) throws JsonProcessingException, IOException, InterruptedException, ExecutionException
    {
//        long startTime = System.currentTimeMillis();
//        System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
//        try{LogManager.getLogManager().readConfiguration();log.setLevel(Level.FINER);} catch ( Exception e ) { e.printStackTrace();}
//        //downloadAll();
//        //puzzleTogether();
//        Scheduler.runDownloader();
//        Scheduler.stopDownloader();
//        log.info("Processing time: "+(System.currentTimeMillis()-startTime)/1000+" seconds. Maximum memory usage of "+memoryBenchmark.updateAndGetMaxMemoryBytes()/1000000+" MB.");
//        System.exit(0); // circumvent non-close bug of ObjectMapper.readTree
        JsonDownloader jdl=new JsonDownloader();
        jdl.run();
    }

}
