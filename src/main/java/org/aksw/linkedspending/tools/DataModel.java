package org.aksw.linkedspending.tools;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import lombok.Getter;

/**
 * Data Model
 */
public class DataModel {

    static public class QB
    {
        @Getter static final String qb = "http://purl.org/linked-data/cube#";
        @Getter static final Resource dataStructureDefinition = ResourceFactory.createResource(qb + "DataStructureDefinition");
        @Getter static final Resource dataSetResource = ResourceFactory.createResource(qb+"DataSet");
        @Getter static final Property dataSet = ResourceFactory.createProperty(qb+"dataSet");
        @Getter static final Property component = ResourceFactory.createProperty(qb+"component");
        @Getter static final Resource dimensionProperty = ResourceFactory.createResource(qb+"DimensionProperty");
        @Getter static final Resource measureProperty = ResourceFactory.createResource(qb+"MeasureProperty");
        @Getter static final Resource attributeProperty = ResourceFactory.createResource(qb+"AttributeProperty");
        @Getter static final Resource sliceKey = ResourceFactory.createResource(qb+"SliceKey");
        @Getter static final Resource hierarchicalCodeList = ResourceFactory.createResource(qb+"HierarchicalCodeList");
        @Getter static final Resource componentSpecification    = ResourceFactory.createResource(qb+"ComponentSpecification");

        @Getter static final Property structure = ResourceFactory.createProperty(qb+"structure");
        @Getter static final Property componentProperty = ResourceFactory.createProperty(qb+"componentProperty");
        @Getter static final Property dimension = ResourceFactory.createProperty(qb+"dimension");
        @Getter static final Property measure = ResourceFactory.createProperty(qb+"measure");
        @Getter static final Property attribute = ResourceFactory.createProperty(qb+"attribute");
        @Getter static final Property concept = ResourceFactory.createProperty(qb+"concept");
        @Getter static final Resource observation = ResourceFactory.createResource(qb+"Observation");
        @Getter static final Resource sliceResource = ResourceFactory.createResource(qb+"Slice");
        @Getter static final Property slice = ResourceFactory.createProperty(qb+"slice");;
        @Getter static final Property sliceStructure = ResourceFactory.createProperty(qb+"sliceStructure");;
        @Getter static final Property parentChildProperty = ResourceFactory.createProperty(qb+"parentChildProperty");
    }

    static public class SDMXMEASURE
    {
        @Getter static final String sdmxMeasure = "http://purl.org/linked-data/sdmx/2009/measure#";
        @Getter static final Property obsValue = ResourceFactory.createProperty(sdmxMeasure+"obsValue");
    }

    static public class SDMXATTRIBUTE
    {
        @Getter static final String sdmxAttribute = "http://purl.org/linked-data/sdmx/2009/attribute#";
        @Getter static final Property currency = ResourceFactory.createProperty(sdmxAttribute+"currency");
        @Getter static final Property refArea = ResourceFactory.createProperty(sdmxAttribute+"refArea");
    }

    static public class SDMXCONCEPT
    {
        @Getter static final String sdmxConcept = "http://purl.org/linked-data/sdmx/2009/concept#";
        @Getter static final Property obsValue = ResourceFactory.createProperty(sdmxConcept+"obsValue");
        @Getter static final Property refPeriod = ResourceFactory.createProperty(sdmxConcept+"refPeriod");
        @Getter static final Property timePeriod = ResourceFactory.createProperty(sdmxConcept+"timePeriod");
    }

    static public class XmlSchema
    {
        @Getter static final String xmlSchema = "http://www.w3.org/2001/XMLSchema#";
        @Getter static final Property gYear = ResourceFactory.createProperty(xmlSchema+"gYear");
    }

    static final public class DBO
    {
        @Getter static final String dbo = "http://dbpedia.org/ontology/";
        @Getter static final public Property currency = ResourceFactory.createProperty(dbo, "currency");
    }

    static final public class DCMI
    {
        @Getter static final String dcmi = "http://dublincore.org/documents/2012/06/14/dcmi-terms/";
        @Getter static final public Property source = ResourceFactory.createProperty(dcmi,"source");
        @Getter static final public Property created = ResourceFactory.createProperty(dcmi,"created");
    }
}