package org.aksw.linkedspending;

import com.hp.hpl.jena.rdf.model.Model;
import lombok.extern.java.Log;
import org.aksw.linkedspending.tools.DataModel;
import org.aksw.linkedspending.tools.PropertiesLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.LogManager;

@Log
public class Converter implements Runnable {

    /** properties */
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");
    private static boolean stopRequested = false;
    private static boolean pauseRequested = false;

    public static void setStopRequested(boolean setTo) { stopRequested = setTo; }
    public static void setPauseRequested(boolean setTo) { pauseRequested = setTo; }

    /**
     * Map for all files to be loaded into the Converter
     */
    static Map<String,File> files = new ConcurrentHashMap<>();

    @Override
    public void run()
    {
        //stopRequested = false;
        //pauseRequested = false;
        long startTime = System.currentTimeMillis();
        try {
            System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
            LogManager.getLogManager().readConfiguration();
        } catch ( IOException e ) {
            log.setLevel(Level.INFO);
            log.warning("Could not read logging configuration: " + e.getMessage());
        }
        try {
            int minExceptions = Integer.parseInt(PROPERTIES.getProperty("minExceptionsForStop"));
            float exceptionStopRatio = Float.parseFloat(PROPERTIES.getProperty("exceptionStopRatio"));
            Main.folder.mkdir();
            // observations use saved datasets so we need the saved names, if we only create the schema we can use the newest dataset names
            SortedSet<String> datasetNames =  JsonDownloader.getSavedDatasetNames();
            // TODO: parallelize
            //        DetectorFactory.loadProfile("languageprofiles");

            //            JsonNode datasets = m.readTree(new URL(DATASETS));
            //            ArrayNode datasetArray = (ArrayNode)datasets.get("datasets");
            int exceptions = 0;
            int offset = 0;
            int i=0;
            int fileexists=0;
            for(final String datasetName : datasetNames) {
                if(stopRequested) { break; }
                else if(pauseRequested) { while(pauseRequested){} }
                i++;
                Model model = DataModel.newModel();
                File file = getDatasetFile(datasetName);
                if(file.exists() && file.length() > 0) {
                    log.finer("skipping already existing file nr " + i + ": " + file);
                    fileexists++;
                    continue;
                }
                try {
                    OutputStream out = new FileOutputStream(file, true);

                    URL url = new URL(PROPERTIES.getProperty("urlInstance") + datasetName);
                    log.info("Dataset nr. "+i+"/"+datasetNames.size()+": "+url);
                    try {
                        Main.createDataset(datasetName, model, out);
                        Main.writeModel(model, out);
                    } catch(Exception e) {
                        exceptions++;
                        Main.deleteDataset(datasetName);
                        Main.faultyDatasets.add(datasetName);
                        log.severe("Error creating dataset "+datasetName+". Skipping.");
                        e.printStackTrace();
                        if(exceptions>=minExceptions&&((double)exceptions/(i+1))>exceptionStopRatio) {
                            log.severe("Too many exceptions ("+exceptions+" out of "+(i+1));
                            Main.shutdown(1);
                        }

                    }
                } catch (IOException e) {

                }
            }
            if(exceptions>=minExceptions&&((double)exceptions/(i+1))>exceptionStopRatio) {
                if(Main.USE_CACHE) {
                    Main.cache.getCacheManager().shutdown();
                }
                log.severe("Too many exceptions ("+exceptions+" out of "+(i+1));
                Main.shutdown(1);
            }
            if(stopRequested) log.info("** CONVERSION STOPPED, STOP REQUESTED: Processed "+(i-offset)+" datasets with "+exceptions+" exceptions and "+fileexists+" already existing ("+(i-exceptions-fileexists)+" newly created)."
                    +"Processing time: "+(System.currentTimeMillis()-startTime)/1000+" seconds, maximum memory usage of "+ Main.memoryBenchmark.updateAndGetMaxMemoryBytes()/1000000+" MB.");
            else log.info("** FINISHED CONVERSION: Processed "+(i-offset)+" datasets with "+exceptions+" exceptions and "+fileexists+" already existing ("+(i-exceptions-fileexists)+" newly created)."
                    +"Processing time: "+(System.currentTimeMillis()-startTime)/1000+" seconds, maximum memory usage of "+ Main.memoryBenchmark.updateAndGetMaxMemoryBytes()/1000000+" MB.");
            if(Main.faultyDatasets.size()>0) log.warning("Datasets with errors which were not converted: "+ Main.faultyDatasets);
        }

        // we must absolutely make sure that the cache is shut down before we leave the program, else cache can become corrupt which is a big time waster
        catch(RuntimeException e) {
            log.log(Level.SEVERE,e.getLocalizedMessage(),e);
            Main.shutdown(1);
        }
        Main.shutdown(0);
    }
    /*
    public static void main(String[] args)
    {
        long startTime = System.currentTimeMillis();
        try {
            System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
            LogManager.getLogManager().readConfiguration();
        } catch ( IOException e ) {
            log.setLevel(Level.INFO);
            log.warning("Could not read logging configuration: " + e.getMessage());
        }
        try {
            int minExceptions = Integer.parseInt(PROPERTIES.getProperty("minExceptionsForStop"));
            float exceptionStopRatio = Float.parseFloat(PROPERTIES.getProperty("exceptionStopRatio"));
            Main.folder.mkdir();
            // observations use saved datasets so we need the saved names, if we only create the schema we can use the newest dataset names
            SortedSet<String> datasetNames =  JsonDownloader.getSavedDatasetNames();
            // TODO: parallelize
            //        DetectorFactory.loadProfile("languageprofiles");

            //            JsonNode datasets = m.readTree(new URL(DATASETS));
            //            ArrayNode datasetArray = (ArrayNode)datasets.get("datasets");
            int exceptions = 0;
            int offset = 0;
            int i=0;
            int fileexists=0;
            for(final String datasetName : datasetNames) {
                i++;
                Model model = DataModel.newModel();
                File file = getDatasetFile(datasetName);
                if(file.exists() && file.length() > 0) {
                    log.finer("skipping already existing file nr " + i + ": " + file);
                    fileexists++;
                    continue;
                }
                try {
                    OutputStream out = new FileOutputStream(file, true);

                    URL url = new URL(PROPERTIES.getProperty("urlInstance") + datasetName);
                    log.info("Dataset nr. "+i+"/"+datasetNames.size()+": "+url);
                    try {
                        Main.createDataset(datasetName, model, out);
                        Main.writeModel(model, out);
                    } catch(Exception e) {
                        exceptions++;
                        Main.deleteDataset(datasetName);
                        Main.faultyDatasets.add(datasetName);
                        log.severe("Error creating dataset "+datasetName+". Skipping.");
                        e.printStackTrace();
                        if(exceptions>=minExceptions&&((double)exceptions/(i+1))>exceptionStopRatio) {
                            log.severe("Too many exceptions ("+exceptions+" out of "+(i+1));
                            Main.shutdown(1);
                        }

                    }
                } catch (IOException e) {

                }
            }
            if(exceptions>=minExceptions&&((double)exceptions/(i+1))>exceptionStopRatio) {
                if(Main.USE_CACHE) {
                    Main.cache.getCacheManager().shutdown();
                }
                log.severe("Too many exceptions ("+exceptions+" out of "+(i+1));
                Main.shutdown(1);
            }
            log.info("** FINISHED CONVERSION: Processed "+(i-offset)+" datasets with "+exceptions+" exceptions and "+fileexists+" already existing ("+(i-exceptions-fileexists)+" newly created)."
                    +"Processing time: "+(System.currentTimeMillis()-startTime)/1000+" seconds, maximum memory usage of "+ Main.memoryBenchmark.updateAndGetMaxMemoryBytes()/1000000+" MB.");
            if(Main.faultyDatasets.size()>0) log.warning("Datasets with errors which were not converted: "+ Main.faultyDatasets);
        }

        // we must absolutely make sure that the cache is shut down before we leave the program, else cache can become corrupt which is a big time waster
        catch(RuntimeException e) {
            log.log(Level.SEVERE,e.getLocalizedMessage(),e);
            Main.shutdown(1);
        }
        Main.shutdown(0);
    }
    */

    /**
     * Gets a file that is already provided by JSON-Downloader to be converted into rdf. Uses ConcurrentHashMap to track all files in Converter.
     * @param name the name of the JSON-file(LS JSON Diff)
     * @return the file to be converted into the tripple-store
     */
	static File getDatasetFile(String name)
	{
		File file = files.get(name);
		if(file==null) files.put(name,file= new File(Main.folder+"/"+name+".nt"));
		return file;
	}
}
