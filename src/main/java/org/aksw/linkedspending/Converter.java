package org.aksw.linkedspending;

import com.hp.hpl.jena.rdf.model.Model;
import lombok.extern.java.Log;
import org.aksw.linkedspending.tools.DataModel;
import org.aksw.linkedspending.tools.PropertiesLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.LogManager;

@Log
public class Converter {

    /** properties */
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");

    /**
     * Map for all files to be loaded into the Converter
     */
    static Map<String,File> files = new ConcurrentHashMap<>();

    public static void main(String[] args) throws MalformedURLException, IOException
    {
        long startTime = System.currentTimeMillis();
        System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
        try{
            LogManager.getLogManager().readConfiguration();
            log.setLevel(Level.INFO);} catch ( RuntimeException e ) { e.printStackTrace();}
        try
        {
            int minExceptions = Integer.parseInt(PROPERTIES.getProperty("minExceptionsForStop"));
            float exceptionStopRatio = Float.parseFloat(PROPERTIES.getProperty("exceptionStopRatio"));
            Main.folder.mkdir();
            // observations use saved datasets so we need the saved names, if we only create the schema we can use the newest dataset names
            SortedSet<String> datasetNames = Main.conversionMode== Main.ConversionMode.SCHEMA_AND_OBSERVATIONS?JsonDownloader.getSavedDatasetNames():JsonDownloader.getDatasetNames();
            // TODO: parallelize
            //        DetectorFactory.loadProfile("languageprofiles");

            //            JsonNode datasets = m.readTree(new URL(DATASETS));
            //            ArrayNode datasetArray = (ArrayNode)datasets.get("datasets");
            int exceptions = 0;
            //            int notexisting = 0;
            int offset = 0;
            int i=0;
            int fileexists=0;
            //            for(i=0;i<Math.min(datasetArray.length(),10);i++)
            //            for(i=5;i<=5;i++)
            for(final String datasetName : datasetNames)
            {
                //                if(!name.contains("orcamento_brasil_2000_2013")) continue;
                //                if(!datasetName.contains("berlin_de")) continue;
                //                if(!datasetName.contains("2011saiki_budget")) continue;

                i++;
                Model model = DataModel.newModel();
                //                Map<String,Property> componentPropertyByName = new HashMap<>();
                //                Map<String,Resource> hierarchyRootByName = new HashMap<>();
                //                Map<String,Resource> codeListByName = new HashMap<>();

                File file = getDatasetFile(datasetName);
                if(file.exists()&&file.length()>0)
                {
                    log.finer("skipping already existing file nr "+i+": "+file);
                    fileexists++;
                    continue;
                }
                try(OutputStream out = new FileOutputStream(file, true))
                {
                    //                    JsonNode dataSetJson = datasetArray.get(i);
                    URL url = new URL(PROPERTIES.getProperty("urlInstance") + datasetName);
                    //                    URL url = new URL(dataSetJson.get("html_url").asText());
                    //                    String name = dataSetJson.get("name").asText();
                    //                    int nrOfEntries = nrEntries(name);
                    //                    if(nrOfEntries==0)
                    //                    {
                    //                        log.warning("no entries found for dataset "+url);
                    //                        continue;
                    //                    }
                    //                    //        URL url = new URL("http://openspending.org/cameroon_visualisation");
                    //                    //                                URL url = new URL("http://openspending.org/berlin_de");
                    //                    //        URL url = new URL("http://openspending.org/bmz-activities");
                    //                    log.info("Dataset nr. "+i+"/"+datasetArray.size()+": "+url);
                    log.info("Dataset nr. "+i+"/"+datasetNames.size()+": "+url);
                    try
                    {
                        Main.createDataset(datasetName, model, out);
                        Main.writeModel(model, out);
                    }
                    catch(Exception e)
//                    catch (NoCurrencyFoundForCodeException | DatasetHasNoCurrencyException | MissingDataException| UnknownMappingTypeException | TooManyMissingValuesException | FileNotFoundException e)
                    {
                        exceptions++;
                        Main.deleteDataset(datasetName);
                        Main.faultyDatasets.add(datasetName);
                        log.severe("Error creating dataset "+datasetName+". Skipping.");
                        e.printStackTrace();
                        if(exceptions>=minExceptions&&((double)exceptions/(i+1))>exceptionStopRatio    )
                        {
                            log.severe("Too many exceptions ("+exceptions+" out of "+(i+1));
                            Main.shutdown(1);
                        }

                    }
                    {
                    }
                }
            }
            //                catch(TooManyMissingValuesException e)
            //                {
            //                    e.printStackTrace();
            //                    log.severe(e.getLocalizedMessage());
            //                    exceptions++;
            if(exceptions>=minExceptions&&((double)exceptions/(i+1))>exceptionStopRatio    )
            {
                if(Main.USE_CACHE) {
                    Main.cache.getCacheManager().shutdown();}
                log.severe("Too many exceptions ("+exceptions+" out of "+(i+1));
                Main.shutdown(1);
            }
            //                }

            //            log.info("Processed "+(i-offset)+" datasets with "+exceptions+" exceptions and "+notexisting+" not existing datasets, "+fileexists+" already existing ("+(i-exceptions-notexisting-fileexists)+" newly created).");
            log.info("** FINISHED CONVERSION: Processed "+(i-offset)+" datasets with "+exceptions+" exceptions and "+fileexists+" already existing ("+(i-exceptions-fileexists)+" newly created)."
                    +"Processing time: "+(System.currentTimeMillis()-startTime)/1000+" seconds, maximum memory usage of "+ Main.memoryBenchmark.updateAndGetMaxMemoryBytes()/1000000+" MB.");
            if(Main.faultyDatasets.size()>0) log.warning("Datasets with errors which were not converted: "+ Main.faultyDatasets);
        }

        // we must absolutely make sure that the cache is shut down before we leave the program, else cache can become corrupt which is a big time waster
        catch(RuntimeException e) {
            log.log(Level.SEVERE,e.getLocalizedMessage(),e);
            Main.shutdown(1);}
        Main.shutdown(0);
    }

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
