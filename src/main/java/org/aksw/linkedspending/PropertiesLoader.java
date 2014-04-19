package org.aksw.linkedspending;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

/**
 * helper class to load property files
 *
 * @author tovo
 */
public class PropertiesLoader {
    /** logger */
    protected final static Logger LOG = LoggerFactory.getLogger(PropertiesLoader.class);
    /** properties */
    private static HashMap<String, Properties> propertiesList = new HashMap<>();

    /**
     * creates properties from file
     * @param filename file name
     * @return properties
     */
    public Properties getProperties(String filename) {
        if(propertiesList.containsKey(filename)) {
            Properties properties = loadProperties(filename);
            propertiesList.put(filename, properties);
        }
        return propertiesList.get(filename);
    }

    /**
     * loads properties from file
     * @param filename file name
     * @return properties
     */
    private Properties loadProperties(String filename) {
        Properties properties = new Properties();
        InputStream propertiesFile = null;
        try {
            propertiesFile = PropertiesLoader.class.getClassLoader().getResourceAsStream(filename);
            if(propertiesFile == null) {
                LOG.error("Could not open " + filename);
            }
            properties.load(propertiesFile);
        } catch (IOException e) {
            LOG.error("Could not load properties from file: " + e.getMessage());
        } finally {
            if(propertiesFile != null) {
                try {
                    propertiesFile.close();
                } catch (IOException e) {
                    LOG.warn("Error closing properties file: " + e.getMessage());
                }
            }
        }
        return properties;
    }
}
