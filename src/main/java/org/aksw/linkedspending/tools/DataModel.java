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
public class DataModel
{
	/**
	 * Creates a new apache jena model
	 *
	 * @return the model
	 */
	static public Model newModel()
	{
		Model model = ModelFactory.createMemModelMaker().createDefaultModel();
		model.setNsPrefix("qb", "http://purl.org/linked-data/cube#");
		model.setNsPrefix("ls", PropertiesLoader.prefixInstance);
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
		static public final String				base					= "http://purl.org/linked-data/cube#";
		@Getter static public final Resource	dataStructureDefinition	= ResourceFactory
																		.createResource(base + "DataStructureDefinition");
		@Getter static public final Resource	dataSetResource			= ResourceFactory.createResource(base + "DataSet");
		@Getter static public final Property	dataSet					= ResourceFactory.createProperty(base + "dataSet");
		@Getter static public final Property	component				= ResourceFactory.createProperty(base + "component");
		@Getter static public final Resource	dimensionProperty		= ResourceFactory.createResource(base + "DimensionProperty");
		@Getter static public final Resource	measureProperty			= ResourceFactory.createResource(base + "MeasureProperty");
		@Getter static public final Resource	attributeProperty		= ResourceFactory.createResource(base + "AttributeProperty");
		@Getter static public final Resource	sliceKey				= ResourceFactory.createResource(base + "SliceKey");
		@Getter static public final Resource	hierarchicalCodeList	= ResourceFactory.createResource(base + "HierarchicalCodeList");
		@Getter static public final Resource	componentSpecification	= ResourceFactory.createResource(base + "ComponentSpecification");

		@Getter static public final Property	structure				= ResourceFactory.createProperty(base + "structure");
		@Getter static public final Property	componentProperty		= ResourceFactory.createProperty(base + "componentProperty");
		@Getter static public final Property	dimension				= ResourceFactory.createProperty(base + "dimension");
		@Getter static public final Property	measure					= ResourceFactory.createProperty(base + "measure");
		@Getter static public final Property	attribute				= ResourceFactory.createProperty(base + "attribute");
		@Getter static public final Property	concept					= ResourceFactory.createProperty(base + "concept");
		@Getter static public final Resource	observation				= ResourceFactory.createResource(base + "Observation");
		@Getter static public final Resource	sliceResource			= ResourceFactory.createResource(base + "Slice");
		@Getter static public final Property	slice					= ResourceFactory.createProperty(base + "slice");
		@Getter static public final Property	sliceStructure			= ResourceFactory.createProperty(base + "sliceStructure");
		@Getter static public final Property	parentChildProperty		= ResourceFactory.createProperty(base + "parentChildProperty");
	}

	/** SDMX Measure */
	static public class SdmxMeasure
	{
		static public final String				base		= "http://purl.org/linked-data/sdmx/2009/measure#";
		@Getter static public final Property	obsValue	= ResourceFactory.createProperty(base + "obsValue");
	}

	/** SDMX Attribute */
	static public class SdmxAttribute
	{
		static public final String				base		= "http://purl.org/linked-data/sdmx/2009/attribute#";
		@Getter static public final Property	currency	= ResourceFactory.createProperty(base + "currency");
		@Getter static public final Property	refArea		= ResourceFactory.createProperty(base + "refArea");
	}

	/** SDMX Concept */
	static public class SdmxConcept
	{
		static public final String				base		= "http://purl.org/linked-data/sdmx/2009/concept#";
		@Getter static public final Property	obsValue	= ResourceFactory.createProperty(base + "obsValue");
		@Getter static public final Property	refPeriod	= ResourceFactory.createProperty(base + "refPeriod");
		@Getter static public final Property	timePeriod	= ResourceFactory.createProperty(base + "timePeriod");
	}

	/** XML Schema */
	static public class XmlSchema
	{
		static public final String				base	= "http://www.w3.org/2001/XMLSchema#";
		@Getter static public final Property	gYear	= ResourceFactory.createProperty(base + "gYear");
	}

	/** Linked Spending ontology */
	static public class LSOntology
	{
		@Getter static public final String		uri								= PropertiesLoader.prefixOntology;
		@Getter static public final Resource	countryComponent				= ResourceFactory.createResource(uri
																				+ "CountryComponentSpecification");
		@Getter static public final Resource	dateComponentSpecification		= ResourceFactory.createResource(uri
																				+ "DateComponentSpecification");
		@Getter static public final Resource	yearComponentSpecification		= ResourceFactory.createResource(uri
																				+ "YearComponentSpecification");
		@Getter static public final Resource	currencyComponentSpecification	= ResourceFactory.createResource(uri
																				+ "CurrencyComponentSpecification");

		@Getter static public final Property	refDate							= ResourceFactory.createProperty(uri + "refDate");
		@Getter static public final Property	refYear							= ResourceFactory.createProperty(uri + "refYear");
		@Getter static public final Property	completeness					= ResourceFactory.createProperty(uri + "completeness");

		@Getter static public final Property	sourceCreated = ResourceFactory.createProperty(uri + "sourceCreated");
		@Getter static public final Property	sourceModified = ResourceFactory.createProperty(uri + "sourceModified");
	}

	/** DBPedia ontology */
	static public final class DBPOntology
	{
		static public final String						base		= "http://dbpedia.org/ontology/";
		@Getter static public final Property	currency	= ResourceFactory.createProperty(base, "currency");
	}

//	/** Dublin Core Metadata Initiative */
//	static public final public class DCMI
//	{
//		static public final String						base	= "http://dublincore.org/documents/2012/06/14/uri-terms/";
//		@Getter static public final public Property	source	= ResourceFactory.createProperty(base, "source");
//		@Getter static public final public Property	created	= ResourceFactory.createProperty(base, "created");
//	}
}