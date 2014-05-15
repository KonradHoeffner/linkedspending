package org.aksw.linkedspending.downloader;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import de.konradhoeffner.commons.MemoryBenchmark;
import lombok.extern.java.Log;
import org.aksw.linkedspending.OpenspendingSoftwareModul;
import org.aksw.linkedspending.tools.EventNotification;
import org.aksw.linkedspending.tools.PropertiesLoader;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.NonNullByDefault;

import java.io.*;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

/** Downloads entry files from openspending.org. Provides the input for and thus has to be run before Converter.
 * Datasets are processed in paralllel. Each dataset with more than {@value #pageSize} entries is split into parts with that many entries. **/
@NonNullByDefault
@Log
@SuppressWarnings("serial")
public class JsonDownloader extends OpenspendingSoftwareModul implements Runnable
{
    // public static boolean finished = false;

    /** helps to merge JSON-parts by representing a relative position in a given parts-file*/
    enum Position {TOP,MID,BOTTOM}
    /** external properties to be used in Project */
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");
    /**testmode that makes Downloader only download a specific dataset*/
    static String TEST_MODE = null;
    /**makes downloader load all files from openspending; opposite concrete file in field toBeDownloaded*/
    private static boolean completeRun = true;
    /**field with one(not shure if several possible too) specific file to be downloaded from openspending; used, when completeRun=false*/
    private static String toBeDownloaded;
    /**maximum number of threads used by downloader*/
    static final int MAX_THREADS = 10;
    /**the initial page size
     * @see #pageSize*/
    static final int INITIAL_PAGE_SIZE = 100;
    /**the maximum number of JSON-objects in the JSON-array of a downloaded file in the parts folder<br>
     * explanation: The downloader loads JSON-files from openspending. The JSON-files are stored in .../json.
     * If the number of entries is bigger than pagesize, the file is split into several parts and stored in the .../json/parts/"pagesize"/"datasetname" folder.
     * Else the file is stored completely in the .../json/"datasetname" file.*/
    static final int pageSize = INITIAL_PAGE_SIZE;
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
    static protected LinkedList<String> finishedDatasets = new LinkedList<>();

    public LinkedList<String> getFinishedDatasets() {return finishedDatasets;}

    static { if(!CACHE.exists()) { CACHE.mkdir(); } }
    static { if(!pathJson.exists()) { pathJson.mkdirs();} }
    static { if(!rootPartsFolder.exists()) { rootPartsFolder.mkdirs(); } }

    /**represents all the empty JSON-files in a set; highly interacts with: emptyDatasetFile<br>
     * is used for example to remove empty datasets from downloading-process
     * @see #emptyDatasetFile */
    @SuppressWarnings("null") static final Set<String> emptyDatasets = Collections.synchronizedSet(new HashSet<String>());
    /**used to provide one statistical value: "the maximum memory used by jvm while downloading*/
    static MemoryBenchmark memoryBenchmark = new MemoryBenchmark();


    //todo accessing cache causes NullPointerException (in readJSONString())

    private static boolean downloadStopped = false;
    public boolean getDownloadStopped() {return downloadStopped;}

    /**The maximum days the downloader is waiting until shutdown.
     * Once a stopRequested=true signal is send to downloader it blocks and tries to finish its last tasks before shutting down.*/
    private static final long    TERMINATION_WAIT_DAYS    = 2;

    /**
     * sets a JSON-file to be downloaded from openspending
     * @param setTo the filename of the JSON-file
     * @see #toBeDownloaded
     */
    public static void setToBeDownloaded(String setTo) {toBeDownloaded = setTo;}

    /**
     * sets whether all files are to be downloaded from openspending
     * @param setTo true if all files are to be downloaded
     * @see #completeRun
     */
    public static void setCompleteRun(boolean setTo) {completeRun = setTo;}

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
    static public File getFile(String datasetName) {return Paths.get(pathJson.getPath(),datasetName).toFile();}

    //todo what is this method for?
    public static @NonNull ArrayNode getResults(String datasetName) throws JsonProcessingException, IOException
    {
        return (ArrayNode)m.readTree(getFile(datasetName)).get("results");
    }

    /**
     * Downloads a set of datasets. datasets over a certain size are downloaded in parts.
     * Uses multithreading futures to download files.
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
                futures.add(service.submit(new DownloadCallable(dataset,i++)));
            }
        }
        ThreadMonitor monitor = new ThreadMonitor(service);
        monitor.start();

        for(Future<Boolean> future : futures)
        {
            try{if(future.get()) {successCount++;}}
            catch(ExecutionException e) {e.printStackTrace();}
        }

        if(stopRequested)
        {
            service.shutdown();
            service.awaitTermination(TERMINATION_WAIT_DAYS, TimeUnit.DAYS);
            while(!service.isShutdown())
            {
                System.out.println("service still there...slepeing");
                try {Thread.sleep(1000);}
                catch(InterruptedException e) {}
            }
            monitor.stopMonitoring();
            //Thread.sleep(120000);
            //deleteUnfinishedDatasets();
            writeUnfinishedDatasetNames();
            return true;
        }

        //cleaning up
        log.info(successCount+" datasets newly created.");
        service.shutdown();
        service.awaitTermination(TERMINATION_WAIT_DAYS, TimeUnit.DAYS);
        monitor.stopMonitoring();
        return false;
    }

    /** After stop has been requested, this method writes all names of unfinished datasets into file named
     * unfinishedDatasetNames. With the help of this file, unfinished dataset files will be deleted before
     * another run is started.
     * @return True, if file has been successfully created. False otherwise. */
    protected static boolean writeUnfinishedDatasetNames()
    {
        SortedSet<String> unfinishedDatasets = new TreeSet<>();
        unfinishedDatasets = datasetNames;
        unfinishedDatasets.removeAll(finishedDatasets);

        try
        {
            File f = new File("unfinishedDatasetNames");
            FileWriter output = new FileWriter(f);

            for(String dataset : unfinishedDatasets)
            {
                output.write(dataset);
                output.append(System.getProperty("line.separator"));
            }
            output.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /** Deletes dataset files which have not been marked as finished from their DownloadCallables.
     * Called as a clean-up after stop has been requested.
     * @return true, if files have been deleted successfully. False, if a FileNotFoundException occured. */
    protected static boolean deleteUnfinishedDatasets()
    {
        File f = new File("unfinishedDatasetNames");
        if(f.isFile() && !f.delete()) return false;
        try
        {
            BufferedReader input = new BufferedReader(new FileReader(f));
            String s = input.readLine();
            while( s != null )
            {
                File g = new File("json/"+s);
                if(g.isFile()) g.delete();
                s = input.readLine();
            }
            f.delete();
        }
        catch(IOException e) {return false;}
        if(!deleteNotEmptyFolder(new File("json/parts"))) return false;
        return true;
    }

    /** Recursively deletes a given folder which can't be exspected to be empty. Used to delete json/parts
     * after a stop has been requested.
     * @return Returns true if parts folder has successfully been deleted, false otherwise.*/
    protected static boolean deleteNotEmptyFolder(File folderToBeDeleted)
    {
        File[] files = folderToBeDeleted.listFiles();
        if(files != null)
        {
            for(File file : files)
            {
                if(file.isDirectory()) deleteNotEmptyFolder(file);
                else file.delete();
            }
        }
        if(!folderToBeDeleted.delete()) return false;
        return true;
    }

    /**
     * Collects all parted Datasets from a specific File
     * @param foldername the Place where the parted Files are found
     * @return returns a Hashmap with all Files found in the given Folder.
     */
    protected static Map<String, File> getDataFiles(File foldername)
    {
        Map<String,File> datasetToFolder = new HashMap<>();
        if(!foldername.exists()) {
            foldername.mkdirs();
        }
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
        //for each pathJson containing parts
        for (String dataset : partData.keySet()) {
            File targetFile = new File(pathJson.getPath() + "/" + dataset);
            File mergeFile = new File(pathJson.getPath() + "/" + dataset + ".tmp");
            if(mergeFile.exists()) {
                mergeFile.delete();
            }

            try (PrintWriter out = new PrintWriter(mergeFile)) {
                int partNr = 0;
                File[] parts = partData.get(dataset).listFiles();
                //for each file in the parts pathJson
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
    protected synchronized static void puzzleTogether()
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
    public static void downloadSpecific(String datasetName) throws IOException, InterruptedException, ExecutionException
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
        if(TEST_MODE != null) {datasetNames=new TreeSet<>(Collections.singleton(TEST_MODE));}
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
        //cleaning up from previously stopped runs
        File f = new File("unfinishedDatasetNames");
        if(f.exists()) deleteUnfinishedDatasets();
        //f.delete();

        //try{Thread.sleep(60000);}
        //catch(InterruptedException e) {e.printStackTrace();}

        long startTime = System.currentTimeMillis();
        try
        {
            if(completeRun)
            {
                eventContainer.getEventNotifications().add(new EventNotification(EventNotification.EventType.startedDownloadingComplete, EventNotification.EventSource.Downloader));
                downloadAll();
                eventContainer.getEventNotifications().add(new EventNotification(EventNotification.EventType.finishedDownloadingComplete, EventNotification.EventSource.Downloader, true));
            }
            else
            {
                eventContainer.getEventNotifications().add(new EventNotification(EventNotification.EventType.startedDownloadingSingle, EventNotification.EventSource.Downloader));
                downloadSpecific(toBeDownloaded);
                eventContainer.getEventNotifications().add(new EventNotification(EventNotification.EventType.finishedDownloadingSingle, EventNotification.EventSource.Downloader, true));
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
