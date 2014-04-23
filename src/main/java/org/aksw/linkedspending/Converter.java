package org.aksw.linkedspending;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;
import de.konradhoeffner.commons.MemoryBenchmark;
import lombok.NonNull;
import lombok.extern.java.Log;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import org.aksw.linkedspending.tools.DataModel;
import org.aksw.linkedspending.tools.Exceptions;
import org.aksw.linkedspending.tools.PropertiesLoader;
import org.eclipse.jdt.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.LogManager;

@Log
public class Converter {

    /** properties */
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");

    static final boolean USE_CACHE = Boolean.parseBoolean(PROPERTIES.getProperty("useCache", "true"));
    static final Cache cache = USE_CACHE?CacheManager.getInstance().getCache("openspending-json"):null;
    static MemoryBenchmark memoryBenchmark = new MemoryBenchmark();
    static File folder = new File("output20143");
    static List<String> faultyDatasets = new LinkedList<>();
    /**
     * Map for all files to be loaded into the Converter
     */
    static Map<String,File> files = new ConcurrentHashMap<>();

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
            folder.mkdir();
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
                        writeModel(model, out);
                    } catch(Exception e) {
                        exceptions++;
                        deleteDataset(datasetName);
                        faultyDatasets.add(datasetName);
                        log.severe("Error creating dataset "+datasetName+". Skipping.");
                        e.printStackTrace();
                        if(exceptions>=minExceptions&&((double)exceptions/(i+1))>exceptionStopRatio) {
                            log.severe("Too many exceptions ("+exceptions+" out of "+(i+1));
                            shutdown(1);
                        }

                    }
                } catch (IOException e) {

                }
            }
            if(exceptions>=minExceptions&&((double)exceptions/(i+1))>exceptionStopRatio) {
                if(USE_CACHE) {
                    cache.getCacheManager().shutdown();
                }
                log.severe("Too many exceptions ("+exceptions+" out of "+(i+1));
                shutdown(1);
            }
            log.info("** FINISHED CONVERSION: Processed "+(i-offset)+" datasets with "+exceptions+" exceptions and "+fileexists+" already existing ("+(i-exceptions-fileexists)+" newly created)."
                    +"Processing time: "+(System.currentTimeMillis()-startTime)/1000+" seconds, maximum memory usage of "+ memoryBenchmark.updateAndGetMaxMemoryBytes()/1000000+" MB.");
            if(faultyDatasets.size()>0) log.warning("Datasets with errors which were not converted: "+ faultyDatasets);
        }

        // we must absolutely make sure that the cache is shut down before we leave the program, else cache can become corrupt which is a big time waster
        catch(RuntimeException e) {
            log.log(Level.SEVERE,e.getLocalizedMessage(),e);
            shutdown(1);
        }
        shutdown(0);
    }

    /**
     * Gets a file that is already provided by JSON-Downloader to be converted into rdf. Uses ConcurrentHashMap to track all files in Converter.
     * @param name the name of the JSON-file(LS JSON Diff)
     * @return the file to be converted into the tripple-store
     */
	static File getDatasetFile(String name)
	{
		File file = files.get(name);
		if(file==null) files.put(name,file= new File(folder+"/"+name+".nt"));
		return file;
	}

    static void shutdown(int status)
    {
        if(USE_CACHE) {
            CacheManager.getInstance().shutdown();}
        System.exit(status);
    }

    static void writeModel(Model model, OutputStream out)
    {
        model.write(out,"N-TRIPLE");
        //        model.write(out,"TURTLE");
        // assuming that most memory is consumed before model cleaning
        memoryBenchmark.updateAndGetMaxMemoryBytes();
        model.removeAll();
    }

    static void deleteDataset(String datasetName)
    {
        System.out.println("******************************++deelte"+datasetName);
        Converter.getDatasetFile(datasetName).delete();
    }

}
