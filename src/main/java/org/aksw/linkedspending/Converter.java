package org.aksw.linkedspending;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;
import org.aksw.linkedspending.tools.PropertiesLoader;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class Converter {

    /** properties */
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");

    /**
     * Map for all files to be loaded into the Converter
     */
    static Map<String,File> files = new ConcurrentHashMap<>();

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

    /**
     * Creates a new apache jena model
     * @return the model
     */
	static Model newModel()
	{
		Model model = ModelFactory.createMemModelMaker().createDefaultModel();
		model.setNsPrefix("qb", "http://purl.org/linked-data/cube#");
		model.setNsPrefix("ls", PROPERTIES.getProperty("urlInstance"));
		model.setNsPrefix("lso", LSO.URI);
		model.setNsPrefix("sdmx-subject",	"http://purl.org/linked-data/sdmx/2009/subject#");
		model.setNsPrefix("sdmx-dimension",	"http://purl.org/linked-data/sdmx/2009/dimension#");
		model.setNsPrefix("sdmx-attribute",	"http://purl.org/linked-data/sdmx/2009/attribute#");
		model.setNsPrefix("sdmx-measure",	"http://purl.org/linked-data/sdmx/2009/measure#");
		model.setNsPrefix("rdfs", RDFS.getURI());
		model.setNsPrefix("rdf", RDF.getURI());
		model.setNsPrefix("xsd", XSD.getURI());
		return model;
	}

    static public class LSO
    {
        static final String URI = PROPERTIES.getProperty("urlOntology");
        static final public Resource CountryComponent = ResourceFactory.createResource(URI + "CountryComponentSpecification");
        static final public Resource DateComponentSpecification = ResourceFactory.createResource(URI+"DateComponentSpecification");
        static final public Resource YearComponentSpecification = ResourceFactory.createResource(URI+"YearComponentSpecification");
        static final public Resource CurrencyComponentSpecification = ResourceFactory.createResource(URI+"CurrencyComponentSpecification");

        static final public Property refDate = ResourceFactory.createProperty(URI+"refDate");
        static final public Property refYear = ResourceFactory.createProperty(URI+"refYear");
        static final public Property completeness = ResourceFactory.createProperty(URI+"completeness");
    }
}
