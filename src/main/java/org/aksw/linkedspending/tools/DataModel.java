package org.aksw.linkedspending.tools;

import lombok.Getter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;

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
		model.setNsPrefix("ls", PropertyLoader.prefixInstance);
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
		static public final String				BASE					= "http://purl.org/linked-data/cube#";
		// in addition to myResource.getURI() because switch statements need constants
		static public final String				DIMENSION_PROPERTY_URI = BASE+"DimensionProperty";
		static public final String				MEASURE_PROPERTY_URI = BASE+"MeasureProperty";
		static public final String				ATTRIBUTE_PROPERTY_URI = BASE+"AttributeProperty";

		static public final Resource	DataStructureDefinition	= ResourceFactory
																		.createResource(BASE + "DataStructureDefinition");
		static public final Resource	DataSet			= ResourceFactory.createResource(BASE + "DataSet");
		static public final Resource	ComponentProperty		= ResourceFactory.createResource(BASE + "ComponentProperty");
		static public final Resource	DimensionProperty		= ResourceFactory.createResource(BASE + "DimensionProperty");
		static public final Resource	MeasureProperty			= ResourceFactory.createResource(BASE + "MeasureProperty");
		static public final Resource	AttributeProperty		= ResourceFactory.createResource(BASE + "AttributeProperty");
		static public final Resource	SliceKey				= ResourceFactory.createResource(BASE + "SliceKey");
		static public final Resource	HierarchicalCodeList	= ResourceFactory.createResource(BASE + "HierarchicalCodeList");
		static public final Resource	ComponentSpecification	= ResourceFactory.createResource(BASE + "ComponentSpecification");
		static public final Resource	Observation				= ResourceFactory.createResource(BASE + "Observation");
		static public final Resource	Slice			= ResourceFactory.createResource(BASE + "Slice");

		static public final Property	component				= ResourceFactory.createProperty(BASE + "component");
		static public final Property	dataSet					= ResourceFactory.createProperty(BASE + "dataSet");
		static public final Property	structure				= ResourceFactory.createProperty(BASE + "structure");
		static public final Property	componentProperty		= ResourceFactory.createProperty(BASE + "componentProperty");
		static public final Property	dimension				= ResourceFactory.createProperty(BASE + "dimension");
		static public final Property	measure					= ResourceFactory.createProperty(BASE + "measure");
		static public final Property	attribute				= ResourceFactory.createProperty(BASE + "attribute");
		static public final Property	concept					= ResourceFactory.createProperty(BASE + "concept");
		static public final Property	slice					= ResourceFactory.createProperty(BASE + "slice");
		static public final Property	sliceStructure			= ResourceFactory.createProperty(BASE + "sliceStructure");
		static public final Property	parentChildProperty		= ResourceFactory.createProperty(BASE + "parentChildProperty");
	}

	static public class Owl
	{
		static public final String				base		= "http://www.w3.org/2002/07/owl#";
		static public final String OBJECT_PROPERTY_URI = base + "ObjectProperty";
		static public final String DATATYPE_PROPERTY_URI = base+ "DatatypeProperty";
	}


	static public class SdmxDimension
	{
		static public final String				base		= "http://purl.org/linked-data/sdmx/2009/dimension#";
		@Getter static public final Property	obsValue	= ResourceFactory.createProperty(base + "obsValue");
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
		@Getter static public final Property	refPeriod	= 	ResourceFactory.createProperty(base + "refPeriod");
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
		@Getter static public final String		uri								= PropertyLoader.prefixOntology;
		@Getter static public final Resource	countryComponentSpecification				= ResourceFactory.createResource(uri
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
		@Getter static public final Property	transformationVersion			= ResourceFactory.createProperty(uri + "transformationVersion");
		@Getter static public final Property	uploadComplete					= ResourceFactory.createProperty(uri + "uploadComplete");
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