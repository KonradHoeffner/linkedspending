package org.aksw.linkedspending.convert;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.NonNull;
import lombok.extern.java.Log;
import org.aksw.linkedspending.LinkedSpendingDatasetInfo;
import org.aksw.linkedspending.OpenSpendingDatasetInfo;
import org.aksw.linkedspending.exception.DataSetDoesNotExistException;
import org.aksw.linkedspending.exception.DatasetHasNoCurrencyException;
import org.aksw.linkedspending.exception.MissingDataException;
import org.aksw.linkedspending.exception.NoCurrencyFoundForCodeException;
import org.aksw.linkedspending.exception.TooManyMissingValuesException;
import org.aksw.linkedspending.exception.UnknownMappingTypeException;
import org.aksw.linkedspending.job.Job;
import org.aksw.linkedspending.job.Phase;
import org.aksw.linkedspending.job.State;
import org.aksw.linkedspending.job.Worker;
import org.aksw.linkedspending.tools.DataModel;
import org.aksw.linkedspending.tools.JsonReader;
import org.aksw.linkedspending.tools.PropertyLoader;
import org.aksw.linkedspending.upload.UploadWorker;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;
import de.konradhoeffner.commons.MemoryBenchmark;
import de.konradhoeffner.commons.Pair;
import de.konradhoeffner.commons.TSVReader;

/** Converts an OpenSpending JSON dataset to RDF Data Cube*/
@Log @NonNullByDefault public class ConvertWorker extends Worker
{
	// in at least one case there is no
	static private final boolean CHANGE_AMOUNT_PROPERTY_TO_MEASURE = true;

	public ConvertWorker(String datasetName, Job job, boolean force)
	{
		super(datasetName, job, force);
	}

	static private Pattern versionPattern = Pattern.compile("\"(\\d*)\"\\^\\^<http://www.w3.org/2001/XMLSchema#int>");

	static final Map<String, String>		codeToCurrency				= new HashMap<>();
	static final Map<Pair<String,String>, String>	userDefinedDatasetPropertyNameToUri	= new HashMap<>();

	private static final boolean	USE_STRING_TO_DATE_NAME_HEURISTIC	= true;
	/** properties */

	static ObjectMapper	m = new ObjectMapper();
	static List<String>	 faultyDatasets	= new LinkedList<>();
	static File statisticsFolder = new File("statistics");
	{
		statisticsFolder.mkdir();
	}
	static File	statistics	= new File(statisticsFolder,"statistics"	+ (System.currentTimeMillis() / 1000));

	/** used to provide one statistical value: "the maximum memory used by jvm while downloading */
	static MemoryBenchmark					memoryBenchmark				= new MemoryBenchmark();
	/**
	 * Map for all files to be loaded into the Converter
	 */
	static Map<String, File>				files						= new ConcurrentHashMap<>();

	//	static
	//	{
	//		if (USE_CACHE)
	//		{
	//			CacheManager.getInstance().addCacheIfAbsent("openspending-json");
	//		}
	//	}

	static
	{
		try (TSVReader in = new TSVReader(ConvertWorker.class.getClassLoader().getResourceAsStream("codetocurrency.tsv")))
		{
			while (in.hasNextTokens())
			{
				String[] tokens = in.nextTokens();
				codeToCurrency.put(tokens[0], tokens[1]);
			}
			in.close();
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	static
	{
		try (TSVReader in = new TSVReader(ConvertWorker.class.getClassLoader().getResourceAsStream("propertymapping.tsv")))
		{
			while (in.hasNextTokens())
			{
				String[] tokens = in.nextTokens();
				userDefinedDatasetPropertyNameToUri.put(new Pair<>(tokens[0], tokens[1]), tokens[2]);
			}
			in.close();
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * gets the names of all files in .../json and returns them
	 *
	 * @return a sorted set of all filenames
	 */
	public static SortedSet<String> getDownloadedDatasetNames()
	{
		try
		{
			if (!PropertyLoader.pathJson.exists()) {PropertyLoader.pathJson.mkdirs();}

			SortedSet<String> names = new TreeSet<>();
			for (File f : PropertyLoader.pathJson.listFiles())
			{
				if (f.isFile())
				{
					names.add(f.getName());
				}
			}
			return names;
		}
		catch (Exception e)
		{
			log.severe("could not access dataset files");
		}
		return null;
	}

	private static Literal instantLiteral(Model model, Instant instant)
	{
		return model.createTypedLiteral(instant.toString(),XSDDatatype.XSDdateTime);
	}

	/**
	 * Takes the url of an openspending dataset and extracts rdf into a jena model.
	 * Each dataset contains a model which gets translated to a datastructure definition and entries
	 * that contain the actual measurements and get translated to a
	 * DataCube.
	 *
	 * @param model
	 *            initialized model that the triples will be added to
	 * @throws java.io.IOException
	 * @throws org.aksw.linkedspending.tools.NoCurrencyFoundForCodeException
	 * @throws org.aksw.linkedspending.tools.DatasetHasNoCurrencyException
	 * @throws org.aksw.linkedspending.tools.UnknownMappingTypeException
	 * @throws org.aksw.linkedspending.tools.TooManyMissingValuesException
	 * @throws InterruptedException
	 * @throws DataSetDoesNotExistException
	 */
	public boolean createDataset(Model model, OutputStream out) throws IOException,
	NoCurrencyFoundForCodeException, DatasetHasNoCurrencyException,
	MissingDataException, UnknownMappingTypeException, TooManyMissingValuesException, InterruptedException, DataSetDoesNotExistException
	{
		@NonNull
		URL url = new URL(PropertyLoader.prefixInstance + datasetName); // linkedspending
		@NonNull
		URL sourceUrl = new URL(PropertyLoader.prefixOpenSpending + datasetName + ".json");
		@NonNull
		JsonNode datasetJson = JsonReader.read(sourceUrl);
		@NonNull
		Resource dataSet = model.createResource(url.toString());
		@NonNull
		Resource dsd = createDataStructureDefinition(new URL(url + "/model"), model);
		model.add(dataSet, DCTerms.source, model.createResource(PropertyLoader.prefixOpenSpending + datasetName));
		{
			Instant modified = Instant.now();
			Instant created = LinkedSpendingDatasetInfo.forDataset(datasetName).map(LinkedSpendingDatasetInfo::getCreated).orElse(modified);

			model.add(dataSet, DCTerms.modified, instantLiteral(model,modified));
			model.add(dataSet, DCTerms.created, instantLiteral(model,created));
		}
		{
			OpenSpendingDatasetInfo osInfo = OpenSpendingDatasetInfo.forDataset(datasetName);
			model.add(dataSet, DataModel.LSOntology.sourceModified, instantLiteral(model, osInfo.modified));
			model.add(dataSet, DataModel.LSOntology.sourceCreated, instantLiteral(model, osInfo.created));
		}

		// currency is defined on the dataset level in openspending but in RDF datacube we decided
		// to define it for each observation
		Resource currency;

		if (datasetJson.has("currency"))
		{
			String currencyCode = datasetJson.get("currency").asText();
			currency = model.createResource(codeToCurrency.get(currencyCode));
			if (currency == null) { throw new NoCurrencyFoundForCodeException(datasetName, currencyCode); }
			model.add(dsd, DataModel.DataCube.component, DataModel.LSOntology.getCurrencyComponentSpecification());

			// model.add(currencyComponent, DataCube.attribute, SdmxAttribute.currency);
			// model.addLiteral(SdmxAttribute.currency, RDFS.label,model.createLiteral("currency"));
			// model.add(SdmxAttribute.currency, RDF.type, RDF.Property);
			// model.add(SdmxAttribute.currency, RDF.type, DataCube.AttributeProperty);
			// //model.add(SdmxAttribute.currency, RDFS.subPropertyOf,SDMXMEASURE.obsValue);
			// model.add(SdmxAttribute.currency, RDFS.range,XSD.decimal);
		}
		else
		{
			log.warning("no currency for dataset " + datasetName + ", skipping");
			throw new DatasetHasNoCurrencyException(datasetName);
		}
		final Integer defaultYear;
		{
			String defaultYearString = cleanString(datasetJson.get("default_time").asText());
			// we only want the year, not date and time which are 1-1 and 0:0:0 anyways
			if (defaultYearString != null) defaultYearString = defaultYearString.substring(0, 4);
			defaultYear = defaultYearString == null ? null : Integer.valueOf(defaultYearString);
		}
		Set<ComponentProperty> componentProperties;
		try
		{
			componentProperties = createComponents(
					JsonReader.read(new URL(PropertyLoader.prefixOpenSpending + datasetName + "/model")).get("mapping"), model,
					datasetName, dataSet, dsd, defaultYear != null);
		}
		catch (MissingDataException | UnknownMappingTypeException e)
		{
			log.severe("Error creating components for dataset " + datasetName);
			throw e;
		}

		model.add(dataSet, RDF.type, DataModel.DataCube.DataSet);
		model.add(dataSet, DataModel.DataCube.structure, dsd);

		List<String> territories = ArrayNodeToStringList((ArrayNode) datasetJson.get("territories"));
		Set<Resource> countries = new HashSet<>();
		@Nullable
		Literal yearLiteral = null;
		if (defaultYear != null)
		{
			model.add(dsd, DataModel.DataCube.component, DataModel.LSOntology.getYearComponentSpecification());
			yearLiteral = model.createTypedLiteral(defaultYear, XSD.gYear.getURI());
			model.add(dataSet, DataModel.LSOntology.getRefYear(), yearLiteral);
		}

		if (!territories.isEmpty())
		{
			model.add(dsd, DataModel.DataCube.component, DataModel.LSOntology.getCountryComponentSpecification());
			for (String territory : territories)
			{
				Resource country = model.createResource(Countries.lgdCountryByCode.get(territory));
				countries.add(country);
				model.add(dataSet, DataModel.SdmxAttribute.getRefArea(), country);
			}
		}
		{
			// JsonNode entries = readJSON(new
			// URL("http://openspending.org/api/2/search?format=json&pagesize="+MAX_ENTRIES+"&dataset="+dataSetName),true);
			// log.fine("extracting results");
			// ArrayNode results = (ArrayNode)entries.get("results");
			log.finer(datasetName+" creating entries");
			if(!createObservations(model, out, dataSet, componentProperties, currency, countries, yearLiteral)) {return false;} // stop requested
		}
		createViews(datasetName, model, dataSet);
		//		List<String> languages = ArrayNodeToStringList((ArrayNode) datasetJson.get("languages"));

		// qb:component [qb:attribute sdmx-attribute:unitMeasure;
		// qb:componentRequired "true"^^xsd:boolean;
		// qb:componentAttachment qb:DataSet;]
		String label = datasetJson.get("label").asText();
		String description = datasetJson.get("description").asText();

		// doesnt work well enough
		// // guess the language for the language tag
		// // we assume that label and description have the same language
		// Detector detector = DetectorFactory.create();
		// detector.append(label);
		// detector.append(description);
		// String language = detector.detect();
		// model.add(dataSet, RDFS.label, label,language);
		// model.add(dataSet, RDFS.comment, description,language);
		model.add(dataSet, RDFS.label, label);
		model.add(dataSet, RDFS.comment, description);
		model.add(dataSet,DCTerms.identifier,datasetName);
		// todo: find out the language
		// model.createStatement(arg0, arg1, arg2)
		return true;
	}

	/**
	 * Takes a url of an openspending dataset model and extracts rdf into a jena model.
	 * The DataStructureDefinition (DSD) specifies the structure of a dataset and contains a set of
	 * qb:ComponentSpecification resources.
	 * Developer note: method doesn't do that much anymore because it most properties add themself to the DSD on creation now
	 * @param url
	 *            url that contains an openspending dataset model, e.g.
	 *            http://openspending.org/fukuoka_2013/model
	 * @param model
	 *            initialized model that the triples will be added to
	 */
	static Resource createDataStructureDefinition(final URL url, Model model) throws IOException
	{
		log.finer("Creating DSD");
		Resource dsd = model.createResource(url.toString());
		model.add(dsd, RDF.type, DataModel.DataCube.DataStructureDefinition);

		// JsonNode dsdJson = readJSON(url);
		// mapping is now gotten in createdataset
		// JsonNode mapping = dsdJson.get("mapping");
		// for(Iterator<String> it = mapping.keys();it.hasNext();)
		// {
		// String key = it.next();
		// JsonNode dimJson = mapping.get(key);
		// String type = dimJson.get("type");
		// switch(type)
		// {
		// case "compound":return;
		// case "measure":return;
		// case "date":return;
		// case "attribute":return;
		// }

		// if(1==1)throw new RuntimeException(dimURL);
		// Resource dim = model.createResource(dimURL);
		// model.add(dim,RDF.type,DataCube.DimensionProperty);

		// String label = dimJson.get("label");
		// if(label!=null&&!label.equals("null")) {model.add(dim,RDFS.label,label);}
		// String description = dimJson.get("description");
		// if(description!=null&&!description.equals("null"))
		// {model.add(dim,RDFS.comment,description);}

		// }

		// if(dsdJson.has("views"))
		// {
		// ArrayNode views = dsdJson.getArrayNode("views");
		// }

		return dsd;
	}

	@Nullable static String cleanString(@Nullable String s)
	{
		if (s == null || "null".equals(s) || s.trim().isEmpty()) return null;
		return s;
	}

	private static Resource dimRange(Model model, String uri)
	{
		return model.createResource(uri+"Class");
	}

	/**
	 * Creates component specifications and adds them to the model. Adds backlinks from their parent DataStructureDefinition.
	 *
	 * @throws org.aksw.linkedspending.tools.UnknownMappingTypeException
	 */
	static Set<ComponentProperty> createComponents(JsonNode mapping, Model model, String datasetName, Resource dataset,
			Resource dsd, boolean datasetHasYear) throws MalformedURLException, IOException, MissingDataException,
	UnknownMappingTypeException
	{
		int attributeCount = 1; // currency is always there and dataset is not created if it is not
		// found
		int dimensionCount = 0;
		int measureCount = 0;
		Map<String, Property> propertyByName = new HashMap<>();
		Set<ComponentProperty> componentProperties = new HashSet<>();
		// ArrayNode dimensionArray = readArrayNode(url);

		for (Iterator<String> it = mapping.fieldNames(); it.hasNext();)
		{
			// JsonNode dimJson = dimensionArray.get(i);
			String key = it.next();
			JsonNode componentJson = mapping.get(key);

			// String name = cleanString(componentJson.get("name"));

			String name = key;
			@NonNull
			String type = cleanString(componentJson.get("type").asText());
			assert type != null;
			if(CHANGE_AMOUNT_PROPERTY_TO_MEASURE&&name.equals("amount")) {type="measure";}
			// String componentPropertyUrl = componentJson.get("html_url");
			String componentPropertyUrl;
			{
				String uri = userDefinedDatasetPropertyNameToUri.get(new Pair<>(datasetName, name));
				componentPropertyUrl = (uri != null) ? uri : DataModel.LSOntology.getUri() +datasetName+"-"+name;
			}
			Property componentProperty = model.createProperty(componentPropertyUrl);
			Resource componentSpecification = model.createResource(componentPropertyUrl + "-spec");
			propertyByName.put(name, componentProperty);
			{
				@Nullable
				String label = cleanString(componentJson.get("label").asText());
				if (label != null)
				{
					model.add(componentProperty, RDFS.label, label);
				}
				else
				{
					label = name;
					if (label != null)
					{
						model.add(componentProperty, RDFS.label, label);
					}
				}
				model.add(componentSpecification, RDFS.label, "specification of " + label);
			}

			model.add(componentProperty, RDF.type, RDF.Property);
			model.add(componentProperty, RDF.type, DataModel.DataCube.ComponentProperty);

			JsonNode datatypeNode = componentJson.get("datatype");
			String datatype = datatypeNode==null?null:cleanString(datatypeNode.asText());
			boolean isStringDate = false;

			Resource range = null;
			if((USE_STRING_TO_DATE_NAME_HEURISTIC&&"string".equals(datatype)&&name.contains("date"))
					&&(!"compound".equals(type)))
			{
				isStringDate = true;
				range = XSD.date;
			}
			else if(datatype!=null)
			{
				switch(datatype)
				{
					case "float":range = XSD.xfloat;break;
					case "double":range = XSD.xdouble;break;
					case "string":range = XSD.xstring;break;
					case "date":range = XSD.date;break;
					//					case "id":range = ..;
					default: range = null;
				}
			}
			if(range!=null)
			{
				model.add(componentProperty,RDFS.range,range);
				model.add(componentProperty, RDF.type, OWL.DatatypeProperty);
			}

			if (componentJson.has("description"))
			{
				String description = cleanString(componentJson.get("description").asText());
				if (description != null)
				{
					model.add(componentProperty, RDFS.comment, description);
				}
			}
			else
			{
				log.warning("no description for " + key);
			}

			switch (type)
			{
				case "date": {
					dimensionCount++;
					componentSpecification = DataModel.LSOntology.getDateComponentSpecification();
					componentProperties.add(new ComponentProperty(DataModel.LSOntology.getRefDate(), name,
							ComponentProperty.Type.DATE,false));
					// it's a dimension
					// model.add(componentSpecification, DataCube.dimension, componentProperty);
					// model.add(componentProperty, RDF.type, DataCube.DimensionProperty);

					// model.add(componentProperty, RDFS.subPropertyOf,SDMXDIMENSION.refPeriod);
					// componentProperties.add(new
					// ComponentProperty(componentProperty,name,ComponentProperty.Type.DATE));

					// concept
					// model.add(componentProperty, DataCube.concept,SDMXCONCEPT.timePeriod);
					// if()
					// model.add(dim, RDFS.range,XmlSchema.gYear);
					break;
				}
				case "compound": {
					dimensionCount++;
					// it's a dimension
					model.add(componentSpecification, DataModel.DataCube.dimension, componentProperty);
					model.add(componentSpecification, RDF.type, DataModel.DataCube.ComponentSpecification);
					model.add(componentProperty, RDF.type, DataModel.DataCube.DimensionProperty);
					model.add(componentProperty, DCTerms.identifier, model.createLiteral(name));
					// create a range class
					model.add(componentProperty, RDFS.range, dimRange(model, componentPropertyUrl));
					model.add(componentProperty, RDF.type, OWL.ObjectProperty);
					// assertTrue(); TODO: assert that the "attributes" of the json are always
					// "name" and "label"
					componentProperties.add(new ComponentProperty(componentProperty, name, ComponentProperty.Type.COMPOUND,true));
					// TODO: model.add(componentProperty, DataCube.concept,SDMXCONCEPT. ???);
					break;
				}
				case "measure": {
					measureCount++;
					model.add(componentSpecification, DataModel.DataCube.measure, componentProperty);
					model.add(componentSpecification, RDF.type, DataModel.DataCube.ComponentSpecification);
					model.add(componentProperty, RDF.type, DataModel.DataCube.MeasureProperty);
					model.add(componentProperty, DCTerms.identifier, model.createLiteral(name));

					if(isStringDate)
					{componentProperties.add(new ComponentProperty(componentProperty, name, ComponentProperty.Type.STRING_DATE,true));}
					else
					{componentProperties.add(new ComponentProperty(componentProperty, name, ComponentProperty.Type.MEASURE,true));}
					// TODO: model.add(componentProperty, DataCube.concept,SDMXCONCEPT. ???);
					break;
				}
				case "attribute": {
					attributeCount++;
					// TODO: attribute the same meaning as in DataCube?
					model.add(componentSpecification, DataModel.DataCube.attribute, componentProperty);
					model.add(componentSpecification, RDF.type, DataModel.DataCube.ComponentSpecification);
					model.add(componentProperty, RDF.type, DataModel.DataCube.AttributeProperty);
					model.add(componentProperty, DCTerms.identifier, model.createLiteral(name));
					if(isStringDate)
					{componentProperties.add(new ComponentProperty(componentProperty, name, ComponentProperty.Type.STRING_DATE,true));}
					else
					{componentProperties.add(new ComponentProperty(componentProperty, name, ComponentProperty.Type.ATTRIBUTE,true));}
					// TODO: model.add(componentProperty, DataCube.concept,SDMXCONCEPT. ???);
					break;
				}
				default:
					throw new UnknownMappingTypeException("unkown type: " + type + "of mapping element "
							+ componentJson);
			}
			// backlink
			model.add(dsd, DataModel.DataCube.component, componentSpecification);
		}
		// if(dateExists||datasetHasYear)
		// {
		//
		// }
		// if(!dateExists) {throw new
		// MissingDataException("No date for dataset "+dataset.getLocalName());}
		if (attributeCount == 0 || measureCount == 0 || dimensionCount == 0)
		{
			throw new MissingDataException(datasetName,"no "
					+ (attributeCount == 0 ? "attributes" : (measureCount == 0 ? "measures" : "dimensions")) + " for dataset "
					+ dataset.getLocalName()); }
		return componentProperties;
	}

	public static List<String> ArrayNodeToStringList(ArrayNode ja)
	{
		List<String> l = new LinkedList<>();
		for (int i = 0; i < ja.size(); i++)
		{
			l.add(ja.get(i).asText());
		}
		return l;
	}

	/**
	 * reads JSON-file from json folder and generates observations
	 * writes observations to output folder
	 * writes statistics
	 * deletes the model! @param url entries url, e.g.
	 * http://openspending.org/berlin_de/entries.json (TODO: or
	 * http://openspending.org/api/2/search?dataset=berlin_de&format=json ?)
	 *
	 * @param componentProperties
	 *            the dimensions which are expected to be values for in all entries.
	 * @param datasetName
	 *            the JSON-file to be read from
	 * @return returns false iff stopped
	 * @throws InterruptedException
	 */

	boolean createObservations(Model model, OutputStream out, Resource dataSet,
			Set<ComponentProperty> componentProperties, @Nullable Resource currency, Set<Resource> countries,
			@Nullable Literal yearLiteral) throws IOException, TooManyMissingValuesException, InterruptedException
	{
		try(ResultsReader in = new ResultsReader(datasetName))
		{
			JsonNode result;
			boolean dateExists = false;
			Set<Integer> years = new HashSet<>();
			int missingForAllProperties = 0;
			int expectedForAllProperties = 0;
			// there is no expectedForProperty, as it's the same for all (number of observations) 
			Map<ComponentProperty, Integer> missingForProperty = new HashMap<>();
			int observations;

			int dateParseErrors=0;
			int dateParseSuccesses=0;
			int dateParseEmpty=0;
			final int MAX_DATE_PARSE_ERROR_SAMPLES = 10;
			List<String> dateParseErrorSamples = new ArrayList<>(MAX_DATE_PARSE_ERROR_SAMPLES);

			for (observations = 0; (result = in.read()) != null; observations++)
			{
				//				pausePoint(this);
				if(stopRequested) {job.setState(State.STOPPED);return false;}

//				String osUri = result.get("html_url").asText();				// old format
				Resource osObservation = model.createResource();
//				String id = osUri.substring(osUri.lastIndexOf('/') + 1); // old format
				String id = result.get("id").asText(); 
				String lsUri = PropertyLoader.prefixInstance + "observation-" + datasetName + "-" + id;
				Resource observation = model.createResource(lsUri);
				model.add(observation, RDFS.label, datasetName + " observation " + id);
				model.add(observation, DataModel.DataCube.dataSet, dataSet);
				model.add(observation, RDF.type, DataModel.DataCube.Observation);
				model.add(observation, DCTerms.source, osObservation);
				// boolean dateExists=false;
				for (ComponentProperty d : componentProperties)
				{
					missingForProperty.put(d, 0);
					expectedForAllProperties++;

					if (!result.has(d.name))
					{
						missingForProperty.put(d, missingForProperty.get(d)+1);
						missingForAllProperties++;
						int minMissing =PropertyLoader.minValuesMissingForStop;
						int maxMissing = PropertyLoader.maxValuesMissingLogged;
						double missingStopRatio = PropertyLoader.datasetMissingStopRatio;
						if (missingForProperty.get(d) <= maxMissing)
						{
							log.warning("no entry for property " + d.name + " at entry " + result);
						}
						if (missingForProperty.get(d) == maxMissing)
						{
							log.warning("more missing entries for property " + d.name + ".");
						}
						if (missingForAllProperties >= minMissing && ((double) missingForAllProperties / expectedForAllProperties >= missingStopRatio))
						{
							faultyDatasets.add(datasetName);
							throw new TooManyMissingValuesException(datasetName, missingForAllProperties);
						}
						continue;
					}

					try
					{
						switch (d.type)
						{
							case COMPOUND: {
								JsonNode jsonDim = result.get(d.name);
								// if(jsonDim==null)
								// {
								// errors++;
								// log.warning("no url for entry "+d.name);
								// continue;
								// }
								if (!jsonDim.has("html_url"))
								{
									log.warning("no url for " + jsonDim);
									missingForAllProperties++;
									continue;
								}
								JsonNode urlNode = jsonDim.get("html_url");
								// todo enhancement: interlinking auf dem label -> besser extern
								// todo enhancement: ressource nicht mehrfach erzeugen - aber aufpassen
								// dass der speicher nicht voll wird! wird wohl nur im datenset gehen

								Resource dimensionValue = model.createResource(urlNode.asText());

								if (jsonDim.has("label"))
								{
									model.addLiteral(dimensionValue, RDFS.label, model.createLiteral(jsonDim.get("label").asText()));
								}
								else
								{
									log.warning("no label for dimension " + d.name + " instance " + dimensionValue);
								}
								model.add(observation, d.property, dimensionValue);
								model.add(dimensionValue,RDF.type,dimRange(model, d.property.getURI()));
								break;
							}
							case ATTRIBUTE: {
								String s = result.get(d.name).asText();
								model.addLiteral(observation, d.property, model.createLiteral(s));
								break;
							}
							case MEASURE: {
								String s = result.get(d.name).asText();
								Literal l;
								try
								{
									l = model.createTypedLiteral(Integer.parseInt(s));
								}
								catch (NumberFormatException e)
								{
									l = model.createLiteral(s);
								}
								model.addLiteral(observation, d.property, l);
								break;
							}
							case STRING_DATE:
							{
								dateExists = true;
								String dateString = result.get(d.name).asText().replaceAll("\\+[0-9][0-9]:[0-9][0-9]", "");
								if(dateString.isEmpty()) {dateParseEmpty++; break;}
								if(dateString.length()==10) dateString+="T00:00:00.00Z";
								try
								{

									Instant instant = Instant.parse(dateString);
									Calendar calendar = Calendar.getInstance();
									calendar.setTimeInMillis(instant.toEpochMilli());
									Literal l = model.createTypedLiteral(calendar);
									model.addLiteral(observation,d.property,l);
									dateParseSuccesses++;
								}
								catch(Exception e)
								{
									dateParseErrors++;
									if(dateParseErrorSamples.size()<MAX_DATE_PARSE_ERROR_SAMPLES) dateParseErrorSamples.add(dateString);
								}
								break;
							}
							case DATE: {
								dateExists = true;
								JsonNode jsonDate = result.get(d.name);
								// String week = date.get("week");
								int year = jsonDate.get("year").asInt();
								String sYear = String.format("%04d",year);
								String sMonth = String.format("%02d",jsonDate.get("month").asInt());
								String sDay = String.format("%02d",jsonDate.get("day").asInt());
								model.addLiteral(observation, DataModel.LSOntology.getRefDate(),
										model.createTypedLiteral(sYear + "-" + sMonth + "-" + sDay, XSD.date.getURI()));
								model.addLiteral(observation, DataModel.LSOntology.getRefYear(),
										model.createTypedLiteral(sYear, XSD.gYear.getURI()));
								years.add(year);
							}
						}

					}
					catch (Exception e)
					{
						e.printStackTrace();
						throw new RuntimeException("problem with componentproperty " + d+ ": " + observation, e);
					}
				}
				//
				// String label = result.get("label");
				//
				// if(label!=null&&!label.equals("null")) {model.add(observation,RDFS.label,label);}
				// else
				// {
				// label = result.get("name");
				// if(label!=null&&!label.equals("null")) {model.add(observation,RDFS.label,label);}
				// }
				// String description = result.get("description");
				// if(description!=null&&!description.equals("null"))
				// {model.add(observation,RDFS.comment,description);}
				//
				// String type = result.get("type");
				// switch(type)
				// {
				// case "date":
				// {
				// model.add(observation, RDFS.subPropertyOf,SDMXDIMENSION.refPeriod);
				// // if()
				// // model.add(dim, RDFS.range,XmlSchema.gYear);
				// return;
				// }
				// case "compound":return;
				// case "measure":return;
				// case "attribute":return;
				// }

				if (currency != null)
				{
					model.add(observation, DataModel.DBPOntology.currency, currency);
				}

				if (yearLiteral != null && !dateExists) // fallback, in case entry doesnt have a date
					// attached we use year of the whole dataset
				{
					model.addLiteral(observation, DataModel.LSOntology.getRefYear(), yearLiteral);
				}
				for (Resource country : countries)
				{
					// add the countries to the observations as well (not just the dataset)
					model.add(observation, DataModel.SdmxAttribute.getRefArea(), country);
				}
				if (model.size() > PropertyLoader.maxModelTriples)
				{
					log.finer("writing triples");
					writeModel(model, out);
				}
			}

			// completeness statistics
			if (expectedForAllProperties == 0)
			{
				log.warning("no observations for dataset " + datasetName + ".");
			}
			else
			{
				model.addLiteral(dataSet, DataModel.LSOntology.getCompleteness(), 1 - (double) (missingForAllProperties / expectedForAllProperties));
				for (ComponentProperty d : componentProperties)
				{
					// only add completeness for properties that are only used by a single RDF data cube
					if(d.isDataSetSpecific)
					{
						model.addLiteral(d.property, DataModel.LSOntology.getCompleteness(),
								1 - (double) (missingForProperty.get(d)/ expectedForAllProperties));
					}
				}

				// in case the dataset goes over several years or doesnt have a default time attached we
				// want all the years of the observations on the dataset
				for (int year : years)
				{
					model.addLiteral(dataSet, DataModel.LSOntology.getRefYear(), model.createTypedLiteral(year, XSD.gYear.getURI()));
				}
				writeModel(model, out);
				// write missing statistics
				try (PrintWriter statisticsOut = new PrintWriter(new BufferedWriter(new FileWriter(statistics, true))))
				{
					if (missingForProperty.isEmpty())
					{
						statisticsOut.println("no missing values");
					}
					else
					{
						statisticsOut.println(datasetName + '\t' + ((double) missingForAllProperties / observations) + '\t'
								+ (double) Collections.max(missingForProperty.values()) / observations);
					}
				}
			}
			log.finer(datasetName+"finished creating entries, "+observations + " observations created.");
			if(dateParseErrors>0)
			{
				log.warning(dateParseErrors+" dateparse errors, "+dateParseSuccesses+" successes, "+dateParseEmpty+" empty, error examples: "+dateParseErrorSamples);
			}
		}
		return true;
	}

	public static void createViews(String datasetName, Model model, Resource dataSet) throws IOException
	{
		ArrayNode views = readArrayNode(new URL(PropertyLoader.prefixOpenSpending + datasetName + "/views.json"));
		for (int i = 0; i < views.size(); i++)
		{
			JsonNode jsonView = views.get(i);
			String name = jsonView.get("name").asText();
			Resource view = model.createResource(PropertyLoader.prefixInstance + datasetName + "/views/" + name);
			model.add(view, RDF.type, DataModel.DataCube.Slice);
			model.add(dataSet, DataModel.DataCube.slice, view);
			String label = jsonView.get("label").asText();
			String description = jsonView.get("description").asText();
			model.add(view, RDFS.label, label);
			model.add(view, RDFS.comment, description);
		}
	}

	static void writeModel(Model model, OutputStream out) throws IOException
	{
		model.write(out, "N-TRIPLE");
		// assuming that most memory is consumed before model cleaning
		MemoryBenchmark.updateAndGetMaxMemoryBytes();
		model.removeAll();
	}

	public static ArrayNode readArrayNode(URL url) throws IOException
	{
		return (ArrayNode) JsonReader.read(url);
		// try {return new ArrayNode(readJSONString(url));}
		// catch(JSONException e) {throw new
		// IOException("Could not create a JSON array from string "+readJSONString(url),e);}
	}

	@Override public Boolean get()
	{
		com.hp.hpl.jena.shared.impl.JenaParameters.enableEagerLiteralValidation=true;
		com.hp.hpl.jena.shared.impl.JenaParameters.enableSilentAcceptanceOfUnknownDatatypes=false;

		job.setPhase(Phase.CONVERT);
		// observations use saved datasets so we need the saved names, if we only create the
		// schema we can use the newest dataset names

		Model model = DataModel.newModel();
		File ntriples = getDatasetFile(datasetName);
		File json = new File(PropertyLoader.pathJson + datasetName+".json");
		boolean skip = false;
		String message = "";
		if(force)
		{
			message = "force is set, not checking for skipping possibility.";
		}
		else if(!(ntriples.exists())) {message = "ntriples file does not exist.";}
		else if(ntriples.length() == 0) {message = "ntriples file is empty.";}
		else if(ntriples.lastModified() <= json.lastModified()) {message = "ntriples file is outdated.";}
		else
		{
			// lsInfo up to date isn't enough because converted data may also be used for dump etc. so we need to inspect the file
			// low tech because we don't want to load a potentially huge model
			try(BufferedReader in = new BufferedReader(new FileReader(ntriples)))
			{
				String line;
				String versionUri = DataModel.LSOntology.transformationVersion.getURI();
				Optional<Integer> version = Optional.empty();
				while((line = in.readLine())!=null)
				{
					if(line.contains(versionUri))
					{
						Matcher m = versionPattern.matcher(line);
						if(m.find())
						{
							version = Optional.of(Integer.valueOf(m.group(1)));
							break;
						}
					}
				}
				if(version.isPresent())
				{
					if(version.get()==UploadWorker.TRANSFORMATION_VERSION)
					{
						skip = true;
						message = "ntriples transformation version of "+version.get()+" is newest one.";
					} else
					{
						message = "ntriples transformation version of "+version.get()
						+" not equal to target version "+UploadWorker.TRANSFORMATION_VERSION+".";
					}
				} else
				{
					message = "no ntriples transformation version found.";
				}
			}
			catch (IOException e)
			{
				String errorMessage = "error when checking whether conversion can be skipped, not skipping: "+Arrays.toString(e.getStackTrace());
				log.severe(errorMessage);
				job.addHistory(errorMessage);
			}
		}
		message = "conversion: "+message+(skip?" Skipping.":" Can't skip -> going ahead with the conversion.");
		log.info(message);
		job.addHistory(message);
		if(skip)
		{
			job.convertProgressPercent.set(100);
			return true;
		}
		//
		//
		////				message = "Converter would like to skip the already existing and up to date file " + ntriples+" but can't because it is not published on LinkedSpending "
		////						+ "and thus we can't be sure that it conforms to the newest transformation.";
		////
		////			} else if(!lsInfo.get().newestTransformation(datasetName))
		////			{
		////				message = "Converter wants to skip already existing and up to date file " + ntriples+" but can't because the transformation version has changed.";
		//			}
		//
		//			log.info(message);
		//			log.info("skipping already existing and up to date file: " + ntriples);
		//			return true;
		//		}

		log.info("Starting conversion of "+datasetName);
		if(ntriples.exists()) {ntriples.delete();}
		try(OutputStream out = new FileOutputStream(ntriples, true))
		{
			if(createDataset(model, out))
			{
				model.add(model.createResource(PropertyLoader.prefixInstance+datasetName),
						DataModel.LSOntology.transformationVersion,
						model.createTypedLiteral(UploadWorker.TRANSFORMATION_VERSION));

				writeModel(model, out);
				log.info("Finished conversion of "+datasetName);
				job.convertProgressPercent.set(100);
				return true;
			}
			else
			{
				log.warning("Stopped conversion of "+datasetName);
				return false;
			}
		}
		catch (Exception e) // Supplier interface doesn't allow checked exceptions
		{
			log.warning("Exception on conversion of "+datasetName+":\n"+Arrays.toString(e.getStackTrace()));
			job.setState(State.FAILED);

			faultyDatasets.add(datasetName);
			throw new RuntimeException("could not convert dataset " + datasetName,e);
		}
	}

	/**
	 * Gets a file that is already provided by JSON-Downloader to be converted into rdf. Uses
	 * ConcurrentHashMap to track all files in Converter.
	 *
	 * @param name
	 *            the name of the JSON-file(LS JSON Diff)
	 * @return the file to be converted into the triple-store
	 */
	static File getDatasetFile(String name)
	{
		File file = files.get(name);
		if (file == null) files.put(name, file = new File(PropertyLoader.pathRdf + "/" + name + ".nt"));
		return file;
	}

}