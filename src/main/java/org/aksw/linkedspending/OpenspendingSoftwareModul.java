package org.aksw.linkedspending;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.java.Log;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import org.aksw.linkedspending.tools.EventNotificationContainer;
import org.aksw.linkedspending.tools.PropertiesLoader;
import org.eclipse.jdt.annotation.NonNullByDefault;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Class to abstract redundant methods and Constants from Downloader and Converter.
 */
@NonNullByDefault
@Log
@SuppressWarnings("serial")
public class OpenspendingSoftwareModul {

    /**whether the cache is used or not*/
    protected static final boolean USE_CACHE = false;// Boolean.parseBoolean(PROPERTIES.getProperty("useCache", "true"));
    /**???is a cache if USE_CACHE=true, otherwise null*/
    protected static final Cache cache = USE_CACHE ? CacheManager.getInstance().getCache("openspending-json") : null;
    /** external properties to be used in Project */
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");
    /**sets downloader stopable*/
    public static boolean stopRequested = false;
    /**pauses downloader until set to false again*/
    public static boolean pauseRequested =false;
    /**object to handle event notifications in hole linkedspending system*/
    protected static EventNotificationContainer eventContainer = new EventNotificationContainer();
    /**used to convert from JSON-file to Java-object and vice versa*/
    protected static ObjectMapper m = new ObjectMapper();
    /**the name of the folder, where the downloaded JSON-files are stored*/
    public static File pathJson = new File(PROPERTIES.getProperty("pathJson"));
    /**the name of the folder, where the downloaded RDF-files are stored*/
    public static File pathRdf = new File(PROPERTIES.getProperty("pathRdf"));
    static {
        try {
            System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
            LogManager.getLogManager().readConfiguration();
        } catch ( IOException e ) {
            log.setLevel(Level.INFO);
            log.warning("Could not read logging configuration: " + e.getMessage());
        }
    }


    /**
     * returns a JSON-string from the given url
     * @param url the url where the JSON-string is located
     * @return a string containing a JSON-object
     * @throws java.io.IOException
     */
    public static String readJSONString(URL url) throws IOException {
        return readJSONString(url, false, USE_CACHE);
    }

    /**
     * returns a JSON-string from the given url
     * @param url the url where the JSON-string is located
     * @param detailedLogging true for better logging
     * @return a string containing a JSON-object
     * @throws java.io.IOException
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
     * @throws java.io.IOException
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
     * @throws com.fasterxml.jackson.core.JsonProcessingException
     * @throws java.io.IOException
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
     * @throws java.io.IOException
     */
    public static JsonNode readJSON(URL url) throws IOException
    {
        return readJSON(url,false);
    }

    /**
     * Reads a JSON-string from an url of openspending. Converts it into a JSON-object
     * Retrieves only one Integer-value from the field:"results_count_query".
     * @param datasetName the name of a dataset on openspending
     * @return the number of results, that the dataset contains
     * @throws java.net.MalformedURLException
     * @throws java.io.IOException
     */
    public static int nrEntries(String datasetName) throws MalformedURLException, IOException
    {
        return readJSON(new URL(PROPERTIES.getProperty("urlOpenSpending") + datasetName + "/entries.json?pagesize=0")).get("stats").get("results_count_query").asInt();
    }

    /**sets the property stopRequested which makes Downloader stoppable,
     * used by scheduler to stop JsonDownloader
     * @param setTo true makes downloader stopable*/
    public static void setStopRequested(boolean setTo) {
        OpenspendingSoftwareModul.stopRequested=setTo;}

    /**
     * sets whether the downloader should stop, even before having finished
     * @param setTo true if downloader shall stop
     * @see OpenspendingSoftwareModul#pauseRequested
     */
    public static void setPauseRequested(boolean setTo) {
        OpenspendingSoftwareModul.pauseRequested = setTo;}

    /**
     * gets event container to deal with events
     * @return the event container
     */
    public static EventNotificationContainer getEventContainer() {return eventContainer;}
}
