package org.aksw.linkedspending.tools;

import lombok.extern.java.Log;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

/**
 * static helper class for loading properties
 */
@Log public final class PropertiesLoader
{
	/** properties */
	private static HashMap<String, Properties>	propertiesList	= new HashMap<>();

	private PropertiesLoader()	{}

	/**
	 * creates properties from file
	 *
	 * @param filename
	 *            file name
	 * @return properties
	 */
	public static Properties getProperties(String filename)
	{
		if (!propertiesList.containsKey(filename))
		{
			Properties properties = loadProperties(filename);
			propertiesList.put(filename, properties);
		}
		return propertiesList.get(filename);
	}

	/**
	 * loads properties from file
	 *
	 * @param filename
	 *            file name
	 * @return properties
	 */
	private static Properties loadProperties(String filename)
	{
		Properties properties = new Properties();
		InputStream propertiesFile = null;
		try
		{
			propertiesFile = PropertiesLoader.class.getClassLoader().getResourceAsStream(filename);
			if (propertiesFile == null)
			{
				log.severe("Could not open " + filename);
			}
			properties.load(propertiesFile);
		}
		catch (IOException e)
		{
			log.severe("Could not load properties from file: " + e.getMessage());
		}
		finally
		{
			if (propertiesFile != null)
			{
				try
				{
					propertiesFile.close();
				}
				catch (IOException e)
				{
					log.warning("Error closing properties file: " + e.getMessage());
				}
			}
		}
		return properties;
	}

	public static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");

	public static final File pathRdf = new File(PROPERTIES.getProperty("pathRdf"));
	public static final File pathJson = new File(PROPERTIES.getProperty("pathJson"));

	public static final String urlInstance = PROPERTIES.getProperty("urlInstance");
	public static final String urlOpenSpending = PROPERTIES.getProperty("urlOpenSpending");

	//	minExceptionsForStop
}