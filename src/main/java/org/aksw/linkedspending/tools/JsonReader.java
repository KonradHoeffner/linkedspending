package org.aksw.linkedspending.tools;

import java.io.IOException;
import java.net.URL;
import java.util.Scanner;
import lombok.extern.java.Log;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Log
public class JsonReader
{
	protected static ObjectMapper m = new ObjectMapper();
	protected static final boolean				USE_CACHE		= false;
	protected static final Cache cache = USE_CACHE ? CacheManager.getInstance().getCache("openspending-json") : null;

	/**
	 * returns a JSON-string from the given url
	 *
	 * @param url
	 *            the url where the JSON-string is located
	 * @return a string containing a JSON-object
	 * @throws java.io.IOException
	 */
	public static String readJSONString(URL url) throws IOException
	{
		return readJSONString(url, false, USE_CACHE);
	}

	/**
	 * returns a JSON-string from the given url
	 *
	 * @param url
	 *            the url where the JSON-string is located
	 * @param detailedLogging
	 *            true for better logging
	 * @return a string containing a JSON-object
	 * @throws java.io.IOException
	 */
	public static String readJSONString(URL url, boolean detailedLogging) throws IOException
	{
		return readJSONString(url, detailedLogging, USE_CACHE);
	}

	/**
	 * reads a JSON-string from openspending and returns it
	 *
	 * @param url
	 *            the url for the string
	 * @param detailedLogging
	 *            true for better logging
	 * @param USE_CACHE
	 * @return a JSON-string
	 * @throws java.io.IOException
	 */
	public static String readJSONString(URL url, boolean detailedLogging, boolean USE_CACHE) throws IOException
	{
		// System.out.println(cache.getKeys());
		if (USE_CACHE)
		{
			Element e = cache.get(url.toString());
			if (e != null)
			{/* System.out.println("cache hit for "+url.toString()); */
				return (String) e.getObjectValue();
			}
		}
		if (detailedLogging)
		{
			log.fine("cache miss for " + url.toString());
		}

		// SWP 14 team: here is a start for the response code handling which you should get to work,
		// I discontinued it because the connection
		// may be a non-httpurlconnection (if the url relates to a file) so maybe there should be
		// two readJsonString methods, one for a file and one for an http url
		// or maybe it should be split into two methods where this one only gets a string as an
		// input and the error handling for connections should be somewhere else
		// of course there shouldn't be System.out.println() statements, they are just placeholders.
		// error handling isnt even that critical here but needs to be in any case in the JSON
		// downloader for the big parts
		// HttpURLConnection connection = (HttpURLConnection)url.openConnection();
		// connection.connect();
		// int response = connection.getResponseCode();
		// switch(response)
		// {
		// case HttpURLConnection.HTTP_OK: System.out.println("OK"); // fine, continue
		// case HttpURLConnection.HTTP_GATEWAY_TIMEOUT: System.out.println("gateway timeout"); //
		// retry
		// case HttpURLConnection.HTTP_UNAVAILABLE: System.out.println("unavailable"); // abort
		// default:
		// log.error("unhandled http response code "+response+". Aborting download of dataset."); //
		// abort
		// }
		// try(Scanner undelimited = new Scanner(connection.getInputStream(), "UTF-8"))
		try (Scanner undelimited = new Scanner(url.openStream(), "UTF-8"))
		{
			try (Scanner scanner = undelimited.useDelimiter("\\A"))
			{
				String datasetsJsonString = scanner.next();
				char firstChar = datasetsJsonString.charAt(0);
				if (!(firstChar == '{' || firstChar == '[')) { throw new IOException("JSON String for URL " + url
						+ " seems to be invalid."); }
				if (USE_CACHE)
				{
					cache.put(new Element(url.toString(), datasetsJsonString));
				}
				// IfAbsent
				return datasetsJsonString;
			}
		}
	}

	/**
	 * reads a JSON-string from an url and converts it into a JSON-object
	 *
	 * @param url
	 *            the url where the JSON-string is located
	 * @param detailedLogging
	 *            true for better logging
	 * @return a JSON-object
	 * @throws com.fasterxml.jackson.core.JsonProcessingException
	 * @throws java.io.IOException
	 */
	public static JsonNode readJSON(URL url, boolean detailedLogging) throws JsonProcessingException, IOException
	{
		String content = readJSONString(url, detailedLogging);
		if (detailedLogging)
		{
			log.fine("finished loading text, creating json object from text");
		}
		return m.readTree(content);
		// try {return new JsonNode(readJSONString(url));}
		// catch(JSONException e) {throw new
		// IOException("Could not create a JSON object from string "+readJSONString(url),e);}
	}

	/**
	 * reads a JSON-string from an url and converts it into a JSON-object
	 *
	 * @param url
	 *            the url where the JSON-string is located
	 * @return a JSON-object
	 * @throws java.io.IOException
	 */
	public static JsonNode read(URL url) throws IOException
	{
		return readJSON(url, false);
	}
}
