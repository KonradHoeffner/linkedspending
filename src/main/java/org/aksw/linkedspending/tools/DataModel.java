package org.aksw.linkedspending.tools;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;
import lombok.Getter;

import java.util.Properties;

/**
 * Data Model
 */
public class DataModel {

    /** properties */
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");

    /**
     * Creates a new apache jena model
     * @return the model
     */
    static public Model newModel()
    {
        Model model = ModelFactory.createMemModelMaker().createDefaultModel();
        model.setNsPrefix("qb", "http://purl.org/linked-data/cube#");
        model.setNsPrefix("ls", PROPERTIES.getProperty("urlInstance"));
        model.setNsPrefix("lso", LSOntology.uri);
        model.setNsPrefix("sdmx-subject", "http://purl.org/linked-data/sdmx/2009/subject#");
        model.setNsPrefix("sdmx-dimension", "http://purl.org/linked-data/sdmx/2009/dimension#");
        model.setNsPrefix("sdmx-attribute", "http://purl.org/linked-data/sdmx/2009/attribute#");
        model.setNsPrefix("sdmx-measure", "http://purl.org/linked-data/sdmx/2009/measure#");
        model.setNsPrefix("rdfs", RDFS.getURI());
        model.setNsPrefix("rdf", RDF.getURI());
        model.setNsPrefix("xsd", XSD.getURI());
        return model;
    }

    /** RDF Data Cube */
    static public class DataCube
    {
        static final String base = "http://purl.org/linked-data/cube#";
        @Getter static final Resource dataStructureDefinition = ResourceFactory.createResource(base + "DataStructureDefinition");
        @Getter static final Resource dataSetResource = ResourceFactory.createResource(base +"DataSet");
        @Getter static final Property dataSet = ResourceFactory.createProperty(base +"dataSet");
        @Getter static final Property component = ResourceFactory.createProperty(base +"component");
        @Getter static final Resource dimensionProperty = ResourceFactory.createResource(base +"DimensionProperty");
        @Getter static final Resource measureProperty = ResourceFactory.createResource(base +"MeasureProperty");
        @Getter static final Resource attributeProperty = ResourceFactory.createResource(base +"AttributeProperty");
        @Getter static final Resource sliceKey = ResourceFactory.createResource(base +"SliceKey");
        @Getter static final Resource hierarchicalCodeList = ResourceFactory.createResource(base +"HierarchicalCodeList");
        @Getter static final Resource componentSpecification = ResourceFactory.createResource(base +"ComponentSpecification");

        @Getter static final Property structure = ResourceFactory.createProperty(base +"structure");
        @Getter static final Property componentProperty = ResourceFactory.createProperty(base +"componentProperty");
        @Getter static final Property dimension = ResourceFactory.createProperty(base +"dimension");
        @Getter static final Property measure = ResourceFactory.createProperty(base +"measure");
        @Getter static final Property attribute = ResourceFactory.createProperty(base +"attribute");
        @Getter static final Property concept = ResourceFactory.createProperty(base +"concept");
        @Getter static final Resource observation = ResourceFactory.createResource(base +"Observation");
        @Getter static final Resource sliceResource = ResourceFactory.createResource(base +"Slice");
        @Getter static final Property slice = ResourceFactory.createProperty(base +"slice");
        @Getter static final Property sliceStructure = ResourceFactory.createProperty(base +"sliceStructure");
        @Getter static final Property parentChildProperty = ResourceFactory.createProperty(base +"parentChildProperty");
    }

    /** SDMX Measure */
    static public class SdmxMeasure
    {
        static final String base = "http://purl.org/linked-data/sdmx/2009/measure#";
        @Getter static final Property obsValue = ResourceFactory.createProperty(base +"obsValue");
    }

    /** SDMX Attribute */
    static public class SdmxAttribute
    {
        static final String base = "http://purl.org/linked-data/sdmx/2009/attribute#";
        @Getter static final Property currency = ResourceFactory.createProperty(base +"currency");
        @Getter static final Property refArea = ResourceFactory.createProperty(base +"refArea");
    }

    /** SDMX Concept */
    static public class SdmxConcept
    {
        static final String base = "http://purl.org/linked-data/sdmx/2009/concept#";
        @Getter static final Property obsValue = ResourceFactory.createProperty(base +"obsValue");
        @Getter static final Property refPeriod = ResourceFactory.createProperty(base +"refPeriod");
        @Getter static final Property timePeriod = ResourceFactory.createProperty(base +"timePeriod");
    }

    /** XML Schema */
    static public class XmlSchema
    {
        static final String base = "http://www.w3.org/2001/XMLSchema#";
        @Getter static final Property gYear = ResourceFactory.createProperty(base +"gYear");
    }

    /** Linked Spending ontology */
    static public class LSOntology
    {
        @Getter static final String uri = PROPERTIES.getProperty("urlOntology");
        @Getter static final Resource countryComponent = ResourceFactory.createResource(uri + "CountryComponentSpecification");
        @Getter static final Resource dateComponentSpecification = ResourceFactory.createResource(uri +"DateComponentSpecification");
        @Getter static final Resource yearComponentSpecification = ResourceFactory.createResource(uri +"YearComponentSpecification");
        @Getter static final Resource currencyComponentSpecification = ResourceFactory.createResource(uri +"CurrencyComponentSpecification");

        @Getter static final Property refDate = ResourceFactory.createProperty(uri +"refDate");
        @Getter static final Property refYear = ResourceFactory.createProperty(uri +"refYear");
        @Getter static final Property completeness = ResourceFactory.createProperty(uri +"completeness");
    }

    /** DBPedia ontology */
    static final public class DBPOntology
    {
        static final String base = "http://dbpedia.org/ontology/";
        @Getter static final public Property currency = ResourceFactory.createProperty(base, "currency");
    }

    /** Dublin Core Metadata Initiative */
    static final public class DCMI
    {
        static final String base = "http://dublincore.org/documents/2012/06/14/uri-terms/";
        @Getter static final public Property source = ResourceFactory.createProperty(base,"source");
        @Getter static final public Property created = ResourceFactory.createProperty(base,"created");
    }
}