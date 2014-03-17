package org.aksw.linkedspending;
import java.io.BufferedWriter;
import static org.aksw.linkedspending.Main.ConversionMode.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.LogManager;
import lombok.NonNull;
import lombok.extern.java.Log;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;
import de.konradhoeffner.commons.MemoryBenchmark;
import de.konradhoeffner.commons.Pair;
import de.konradhoeffner.commons.TSVReader;

// bug? berlin_de doesnt have any measure (should at least have "amount") on sparql endpoint 

/** The main application which does the conversion. Requires JSONDownloader.main() to be called first in order for the input files to exist.
 * Output consists of ntriples files in the chosen folder.**/
@NonNullByDefault
@Log
@SuppressWarnings("serial")
public class Main
{
	enum ConversionMode {SCHEMA_ONLY,SCHEMA_AND_OBSERVATIONS};
	static final ConversionMode conversionMode = ConversionMode.SCHEMA_ONLY;
	
	static MemoryBenchmark memoryBenchmark = new MemoryBenchmark();
	static ObjectMapper m = new ObjectMapper();	
	static final int MAX_MODEL_TRIPLES = 500_000;
	static final boolean USE_CACHE = true;
	public static final String DATASETS = "https://openspending.org/datasets.json";	
	static final String LSBASE = "http://linkedspending.aksw.org/";
	static final String LS = LSBASE+"instance/";
	//	static final String LSO = LSBASE+"ontology/";
	static final String OS = "https://openspending.org/";

	private static final int	MAX_ENTRIES	= Integer.MAX_VALUE;
	//	private static final int	MAX_ENTRIES	= 30;
	private static final int	DATASET_MIN_VALUES_MISSING_FOR_STOP	= 1000;
	private static final int	DATASET_MAX_VALUES_MISSING_LOGGED	= 2;
	private static final float	DATASET_MISSING_STOP_RATIO = 1f;

	private static final int	MIN_EXCEPTIONS_FOR_STOP	= 5;
	private static final float	EXCEPTION_STOP_RATIO	= 0.3f;
	static List<String> faultyDatasets = new LinkedList<>();

	static File folder = new File("outputschema");
	static File statistics = new File("statistics"+(System.currentTimeMillis()/1000));
		
	//	static final boolean CACHING = true;
	static {		
		if(USE_CACHE) {CacheManager.getInstance().addCacheIfAbsent("openspending-json");}
	}
	static final Cache cache = USE_CACHE?CacheManager.getInstance().getCache("openspending-json"):null;

	static final Map<String,String> codeToCurrency = new HashMap<>();
	static
	{
		try(TSVReader in = new TSVReader(Main.class.getClassLoader().getResourceAsStream("codetocurrency.tsv")))
		{
			while(in.hasNextTokens())
			{
				String[] tokens = in.nextTokens();
				codeToCurrency.put(tokens[0], tokens[1]);
			}
		}
		catch (Exception e) {throw new RuntimeException(e);}
	}
	
	static final Map<Pair<String>,String> datasetPropertyNameToUri = new HashMap<>();
	static
	{
		try(TSVReader in = new TSVReader(Main.class.getClassLoader().getResourceAsStream("propertymapping.tsv")))
		{
			while(in.hasNextTokens())
			{
				String[] tokens = in.nextTokens();
				datasetPropertyNameToUri.put(new Pair<String>(tokens[0], tokens[1]),tokens[2]);
			}
		}
		catch (Exception e) {throw new RuntimeException(e);}
	}
	
	static public class NoCurrencyFoundForCodeException 			extends Exception {public NoCurrencyFoundForCodeException(String datasetName, String code) {super("no currency found for code "+code+" in dataset "+datasetName);}}	
	static public class DatasetHasNoCurrencyException 			extends Exception {public DatasetHasNoCurrencyException(String datasetName) {super("dataset "+datasetName+" has no currency.");}}
	static public class MissingDataException 			extends Exception {public MissingDataException(String s) 			{super(s);}}
	static public class UnknownMappingTypeException 	extends Exception {public UnknownMappingTypeException(String s) 	{super(s);}}	
	static public class TooManyMissingValuesException extends Exception
	{public TooManyMissingValuesException(String datasetName, int i) {super(i+" missing values in dataset "+datasetName);}}

	static public class QB
	{
		static final String qb = "http://purl.org/linked-data/cube#";
		static final Resource DataStructureDefinition = ResourceFactory.createResource(qb+"DataStructureDefinition");
		static final Resource DataSet = ResourceFactory.createResource(qb+"DataSet");
		static final Property dataSet = ResourceFactory.createProperty(qb+"dataSet");
		static final Property component = ResourceFactory.createProperty(qb+"component");
		static final Resource DimensionProperty = ResourceFactory.createResource(qb+"DimensionProperty");
		static final Resource MeasureProperty = ResourceFactory.createResource(qb+"MeasureProperty");
		static final Resource AttributeProperty = ResourceFactory.createResource(qb+"AttributeProperty");
		static final Resource SliceKey = ResourceFactory.createResource(qb+"SliceKey");
		static final Resource HierarchicalCodeList = ResourceFactory.createResource(qb+"HierarchicalCodeList");
		static final Resource ComponentSpecification	= ResourceFactory.createResource(qb+"ComponentSpecification");

		static final Property structure = ResourceFactory.createProperty(qb+"structure");
		static final Property componentProperty = ResourceFactory.createProperty(qb+"componentProperty");
		static final Property dimension = ResourceFactory.createProperty(qb+"dimension");
		static final Property measure = ResourceFactory.createProperty(qb+"measure");
		static final Property attribute = ResourceFactory.createProperty(qb+"attribute");
		static final Property concept = ResourceFactory.createProperty(qb+"concept");
		static final Resource Observation	= ResourceFactory.createResource(qb+"Observation");
		static final Resource Slice	= ResourceFactory.createResource(qb+"Slice");
		static final Property slice	= ResourceFactory.createProperty(qb+"slice");;
		static final Property sliceStructure	= ResourceFactory.createProperty(qb+"sliceStructure");;
		static final Property parentChildProperty = ResourceFactory.createProperty(qb+"parentChildProperty");

	}

	static public class SDMXMEASURE
	{
		static final String sdmxMeasure = "http://purl.org/linked-data/sdmx/2009/measure#";
		static final Property obsValue = ResourceFactory.createProperty(sdmxMeasure+"obsValue");		
	}

	static public class SDMXATTRIBUTE
	{
		static final String sdmxAttribute = "http://purl.org/linked-data/sdmx/2009/attribute#";
		//		static final Property currency = ResourceFactory.createProperty(sdmxAttribute+"currency");
		static final Property	refArea	= ResourceFactory.createProperty(sdmxAttribute+"refArea");
	}

	static public class SDMXCONCEPT
	{
		static final String sdmxConcept = "http://purl.org/linked-data/sdmx/2009/concept#";
		static final Property obsValue = ResourceFactory.createProperty(sdmxConcept+"obsValue");		
		//		static final Property refPeriod = ResourceFactory.createProperty(sdmxConcept+"refPeriod");
		//		static final Property timePeriod = ResourceFactory.createProperty(sdmxConcept+"timePeriod");
	}

	static public class XmlSchema
	{
		static final String xmlSchema = "http://www.w3.org/2001/XMLSchema#";
		static final Property gYear = ResourceFactory.createProperty(xmlSchema+"gYear");
	}

	static public class LSO
	{
		static final String URI = LSBASE+"ontology/";
		static final public Resource CountryComponent = ResourceFactory.createResource(URI+"CountryComponentSpecification");
		static final public Resource DateComponentSpecification = ResourceFactory.createResource(URI+"DateComponentSpecification");
		static final public Resource YearComponentSpecification = ResourceFactory.createResource(URI+"YearComponentSpecification");
		static final public Resource CurrencyComponentSpecification = ResourceFactory.createResource(URI+"CurrencyComponentSpecification");

		static final public Property refDate = ResourceFactory.createProperty(URI+"refDate");
		static final public Property refYear = ResourceFactory.createProperty(URI+"refYear");
		static final public Property completeness = ResourceFactory.createProperty(URI+"completeness");
	}

	static final public class DBO
	{
		static final String DBO = "http://dbpedia.org/ontology/";
		static final public Property currency = ResourceFactory.createProperty(DBO,"currency");		
	}

	static final public class DCMI
	{
		static final String DCMI = "http://dublincore.org/documents/2012/06/14/dcmi-terms/";
		static final public Property source = ResourceFactory.createProperty(DCMI,"source");		
		static final public Property created = ResourceFactory.createProperty(DCMI,"created");
	}

	@Nullable static String cleanString(@Nullable String s)
	{
		if(s==null||"null".equals(s)||s.trim().isEmpty()) return null;
		return s;
	}

	/** Creates component specifications. Adds backlinks from their parent DataStructureDefinition.
	 * @throws UnknownMappingTypeException */
	static Set<ComponentProperty> createComponents(JsonNode mapping, Model model,String datasetName, Resource dataset, Resource dsd, boolean datasetHasYear) throws MalformedURLException, IOException, MissingDataException, UnknownMappingTypeException 
	{
		int attributeCount = 1; // currency is always there and dataset is not created if it is not found
		int dimensionCount = 0;
		int measureCount = 0;
		Map<String,Property> componentPropertyByName = new HashMap<>();
		Set<ComponentProperty> componentProperties = new HashSet<>();
		//		ArrayNode dimensionArray = readArrayNode(url);		
		boolean dateExists = false;
		for(Iterator<String> it = mapping.fieldNames();it.hasNext();)
		{
			//			JsonNode dimJson = dimensionArray.get(i);
			String key = it.next();
			JsonNode componentJson = mapping.get(key);

			//			String name = cleanString(componentJson.get("name"));

			String name = key;
			@NonNull String type = cleanString(componentJson.get("type").asText());
			assert type!=null;
			@Nullable String label = cleanString(componentJson.get("label").asText());

			//			String componentPropertyUrl = componentJson.get("html_url");
			String componentPropertyUrl;
			
			String uri = datasetPropertyNameToUri.get(new Pair<String>(datasetName,name));
			componentPropertyUrl=(uri!=null)?uri:LSO.URI+name;			

			Property componentProperty = model.createProperty(componentPropertyUrl);				
			componentPropertyByName.put(name, componentProperty);

			Resource componentSpecification = model.createResource(componentPropertyUrl+"-spec");			
			model.add(componentSpecification, RDFS.label, "specification of "+label);

			model.add(componentProperty, RDF.type, RDF.Property);

			if(label!=null) {model.add(componentProperty,RDFS.label,label);}
			else
			{
				label = name;
				if(label!=null) {model.add(componentProperty,RDFS.label,label);}
			}

			if(componentJson.has("description"))
			{
				String description = cleanString(componentJson.get("description").asText());
				if(description!=null) {model.add(componentProperty,RDFS.comment,description);}
			} else {log.warning("no description for "+key);}

			switch(type)
			{
				case "date":
				{
					dateExists = true;
					dimensionCount++;
					componentSpecification = LSO.DateComponentSpecification;
					componentProperties.add(new ComponentProperty(LSO.refDate,name,ComponentProperty.Type.DATE));
					// it's a dimension
					//					model.add(componentSpecification, QB.dimension, componentProperty);
					//					model.add(componentProperty, RDF.type, QB.DimensionProperty);

					//					model.add(componentProperty, RDFS.subPropertyOf,SDMXDIMENSION.refPeriod);
					//					componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.DATE));

					// concept
					//					model.add(componentProperty, QB.concept,SDMXCONCEPT.timePeriod);  
					//						if()
					//						model.add(dim, RDFS.range,XmlSchema.gYear);
					break;
				}
				case "compound":
				{
					dimensionCount++;
					// it's a dimension
					model.add(componentSpecification, QB.dimension, componentProperty);
					model.add(componentSpecification, RDF.type, QB.ComponentSpecification);
					model.add(componentProperty, RDF.type, QB.DimensionProperty);
					//						assertTrue(); TODO: assert that the "attributes" of the json are always "name" and "label"
					componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.COMPOUND));
					//TODO: model.add(componentProperty, QB.concept,SDMXCONCEPT. ???); 
					break;
				}
				case "measure":
				{
					measureCount++;
					model.add(componentSpecification, QB.measure, componentProperty);
					model.add(componentSpecification, RDF.type, QB.ComponentSpecification);
					model.add(componentProperty, RDF.type, QB.MeasureProperty);

					componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.MEASURE));
					//TODO: model.add(componentProperty, QB.concept,SDMXCONCEPT. ???);
					break;
				}
				case "attribute":
				{
					attributeCount++;
					// TODO: attribute the same meaning as in DataCube?
					model.add(componentSpecification, QB.attribute, componentProperty);
					model.add(componentSpecification, RDF.type, QB.ComponentSpecification);
					model.add(componentProperty, RDF.type, QB.AttributeProperty);

					componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.ATTRIBUTE));
					//TODO: model.add(componentProperty, QB.concept,SDMXCONCEPT. ???);
					break;
				}
				default: throw new UnknownMappingTypeException("unkown type: "+type+"of mapping element "+componentJson);
			}
			// backlink
			model.add(dsd, QB.component, componentSpecification);
		}
		//		if(dateExists||datasetHasYear)
		//		{
		//			
		//		}
		//if(!dateExists) {throw new MissingDataException("No date for dataset "+dataset.getLocalName());}
		if(attributeCount==0||measureCount==0||dimensionCount==0)
		{throw new MissingDataException("no "+(attributeCount==0?"attributes":(measureCount==0?"measures":"dimensions"))+" for dataset "+dataset.getLocalName());}
		return componentProperties;
	}

	//	static void createViews(URL url, Model model) throws MalformedURLException, IOException
	//	{
	//		ArrayNode views = readArrayNode(new URL(url+".json"));
	//		for(int i=0;i<views.length();i++)
	//		{
	//			JsonNode view = views.get(i);
	//
	//			String name = view.get("name");
	//			String label = view.get("label");
	//			String description = view.get("description");
	//
	//			JsonNode state = view.get("state");			
	//			String year = state.get("year"); // TODO: what to do with the year?
	//			Integer.valueOf(year); // throws an exception if its not an integer
	//
	//			JsonNode cuts = state.get("cuts");
	//			List<String> drilldowns = ArrayNodeToStringList(state.getArrayNode("drilldowns"));
	//
	//			Resource slice = model.createResource(url+"/slice/"+name);
	//			{
	//				model.add(slice,RDF.type,QB.Slice);
	//				//			model.add(slice,QB.sliceStructure,...); // TODO
	//				//				model.add(slice,QB.sliceStructure,...);
	//				model.add(slice,RDFS.label,model.createLiteral(label));
	//				model.add(slice,RDFS.comment,model.createLiteral(description));
	//			}
	//			{
	//				Resource sliceKey = model.createResource(url+"/"+name);
	//				model.add(sliceKey,RDF.type,QB.SliceKey);
	//				model.add(sliceKey,RDFS.label,model.createLiteral(label));
	//				model.add(sliceKey,RDFS.comment,model.createLiteral(description));
	//				for(Iterator<String> it = cuts.keys(); it.hasNext();)
	//				{
	//					String dimensionName = it.next();
	//					String dimensionValue = cuts.get(dimensionName);
	//					model.add(sliceKey,QB.componentProperty,componentPropertyByName.get(dimensionName));
	//					model.add(slice,componentPropertyByName.get(dimensionName),dimensionValue);
	//				}
	//
	//			}
	//
	//
	//		}
	//	}

	static void createCodeLists(ArrayNode views, Model model) throws MalformedURLException, IOException
	{
		//		for(int i=0;i<views.length();i++)
		//		{
		//		}
	}

	//	static void createViews(URL datasetUrl, ArrayNode views, Model model,Map<String,Property> componentPropertyByName) throws MalformedURLException, IOException
	//	{		
	//		for(int i=0;i<views.length();i++)
	//		{
	//			JsonNode view = views.get(i);
	//			String entity = view.get("entity");			
	//			String name = view.get("name");
	//			String drilldownName = view.get("drilldown");
	//			Property drilldownProperty = componentPropertyByName.get("drilldown");
	//
	//			String label = view.get("label");
	//			String description = view.get("description");
	//
	//			switch(entity)
	//			{
	//				case "dataset":
	//				{
	//					// create the code list
	//					Resource codeList = model.createResource(datasetUrl+"/codelists/"+name);
	//					model.add(codeList,RDF.type,QB.HierarchicalCodeList);
	//					model.add(codeList,RDFS.label,model.createLiteral("code list for property "+drilldownName,"en"));
	//					codeListByName.put(drilldownName, codeList);					
	//					break;
	//				}
	//				case "dimension":
	//				{
	//					Property dimension = componentPropertyByName.get("dimension");
	//					model.add(codeListByName.get(dimension),QB.parentChildProperty,drilldownProperty);
	//					break;
	//				}
	//				default: throw new RuntimeException("unknown entity value: "+entity+" for view "+view);
	//			}
	//
	//			JsonNode state = view.get("state");			
	//			String year = state.get("year"); // TODO: what to do with the year?
	//			Integer.valueOf(year); // throws an exception if its not an integer
	//
	//			JsonNode cuts = state.get("cuts");
	//			List<String> drilldowns = ArrayNodeToStringList(state.getArrayNode("drilldowns"));
	//
	//			//			Resource slice = model.createResource(datasetUrl+"/slice/"+name);
	//			//			{
	//			//				model.add(slice,RDF.type,QB.Slice);
	//			//				//			model.add(slice,QB.sliceStructure,...); // TODO
	//			//				//				model.add(slice,QB.sliceStructure,...);
	//			//				model.add(slice,RDFS.label,model.createLiteral(label));
	//			//				model.add(slice,RDFS.comment,model.createLiteral(description));
	//			//			}
	//			//			{
	//			//				Resource sliceKey = model.createResource(datasetUrl+"/slicekey/"+name);
	//			//				model.add(sliceKey,RDF.type,QB.SliceKey);
	//			//				model.add(sliceKey,RDFS.label,model.createLiteral(label));
	//			//				model.add(sliceKey,RDFS.comment,model.createLiteral(description));
	//			//				for(Iterator<String> it = cuts.keys(); it.hasNext();)
	//			//				{
	//			//					String dimensionName = it.next();
	//			//					String dimensionValue = cuts.get(dimensionName);
	//			//					model.add(sliceKey,QB.componentProperty,componentPropertyByName.get(dimensionName));
	//			//					model.add(slice,componentPropertyByName.get(dimensionName),dimensionValue);
	//			//				}
	//			//
	//			//			}
	//
	//
	//		}
	//	}
	/**deletes the model! @param url entries url, e.g. http://openspending.org/berlin_de/entries.json	 (TODO: or http://openspending.org/api/2/search?dataset=berlin_de&format=json ?) 
	 * @param componentProperties the dimensions which are expected to be values for in all entries. 
	 * @param countries 
	 * @param defaultYear default year in case no other date is given */

	static void createObservations(String datasetName,Model model,OutputStream out, Resource dataSet, Set<ComponentProperty> componentProperties,@Nullable Resource currency, Set<Resource> countries,@Nullable Literal yearLiteral)
			throws MalformedURLException, IOException, TooManyMissingValuesException
			{
		JsonDownloader.ResultsReader in = new JsonDownloader.ResultsReader(datasetName);
		JsonNode result;
		boolean dateExists = false;
		Set<Integer> years = new HashSet<Integer>();
		int missingValues = 0;
		int expectedValues = 0;
		Map<ComponentProperty,Integer> missingForProperty = new HashMap<>();
		int i;
		for(i=0;(result=in.read())!=null;i++)		
		{			
			String osUri = result.get("html_url").asText();
			Resource osObservation = model.createResource();
			String suffix = osUri.substring(osUri.lastIndexOf('/')+1);
			String lsUri = LS+"observation-"+datasetName+"-"+suffix;
			Resource observation = model.createResource(lsUri);		
			model.add(observation, RDFS.label, datasetName+"// TODO Auto-generated method stub, observation "+suffix);
			model.add(observation, QB.dataSet, dataSet);			
			model.add(observation, RDF.type, QB.Observation);
			model.add(observation,DCMI.source,osObservation);
			//			boolean dateExists=false;
			for(ComponentProperty d: componentProperties)
			{
				//				if(d.name==null) {throw new RuntimeException("no name for component property "+d);}
				expectedValues++;
				if(!result.has(d.name))
				{
					Integer missing = missingForProperty.get(d);
					missing = (missing==null)?1:missing+1;					
					missingForProperty.put(d,missing);
					missingValues++;
					if(missingForProperty.get(d)<=DATASET_MAX_VALUES_MISSING_LOGGED) {log.warning("no entry for property "+d.name+" at entry "+result);}
					if(missingForProperty.get(d)==DATASET_MAX_VALUES_MISSING_LOGGED) {log.warning("more missing entries for property "+d.name+".");}
					if(missingValues>=DATASET_MIN_VALUES_MISSING_FOR_STOP&&((double)missingValues/expectedValues>=DATASET_MISSING_STOP_RATIO)) {faultyDatasets.add(datasetName);throw new TooManyMissingValuesException(datasetName,missingValues);}
					continue;
				}				
				try
				{
					switch(d.type)
					{
						case COMPOUND:
						{
							JsonNode jsonDim = result.get(d.name);							
							//							if(jsonDim==null)
							//							{
							//								errors++;
							//								log.warning("no url for entry "+d.name);
							//								continue;
							//							}
							if(!jsonDim.has("html_url"))
							{
								log.warning("no url for "+jsonDim);
								missingValues++;
								continue;
							}
							JsonNode urlNode = jsonDim.get("html_url");
// todo enhancement: interlinking auf dem label -> besser extern
// todo enhancement: ressource nicht mehrfach erzeugen - aber aufpassen dass der speicher nicht voll wird! wird wohl nur im datenset gehen

							Resource instance = model.createResource(urlNode.asText());

							if(jsonDim.has("label")) {model.addLiteral(instance,RDFS.label,model.createLiteral(jsonDim.get("label").asText()));}
							else	{log.warning("no label for dimension "+d.name+" instance "+instance);}
							model.add(observation,d.property,instance);

							break;
						}
						case ATTRIBUTE:
						{
							String s = result.get(d.name).asText();
							model.addLiteral(observation,d.property,model.createLiteral(s));			
							break;
						}
						case MEASURE:
						{
							String s = result.get(d.name).asText();
							Literal l;
							try {l = model.createTypedLiteral(Integer.parseInt(s));}
							catch(NumberFormatException e)
							{l=model.createLiteral(s);}
							model.addLiteral(observation,d.property,l);
							break;
						}
						case DATE:
						{
							dateExists=true;
							JsonNode jsonDate = result.get(d.name);
							//							String week = date.get("week");
							int year = jsonDate.get("year").asInt();
							int month = jsonDate.get("month").asInt();
							int day = jsonDate.get("day").asInt();							
							model.addLiteral(observation,LSO.refDate,model.createTypedLiteral(year+"-"+month+"-"+day, XSD.date.getURI()));
							model.addLiteral(observation,LSO.refYear,model.createTypedLiteral(year, XSD.gYear.getURI()));
							years.add(year);
						}
					}

				}
				catch(Exception e)
				{					
					throw new RuntimeException("problem with componentproperty "+d.name+": "+observation,e);
				}
			}
			//			
			//			String label = result.get("label");
			//
			//			if(label!=null&&!label.equals("null")) {model.add(observation,RDFS.label,label);}
			//			else
			//			{
			//				label = result.get("name");
			//				if(label!=null&&!label.equals("null")) {model.add(observation,RDFS.label,label);}
			//			}
			//			String description = result.get("description");
			//			if(description!=null&&!description.equals("null")) {model.add(observation,RDFS.comment,description);}				
			//
			//			String type = result.get("type");
			//			switch(type)
			//			{
			//				case "date":
			//				{
			//					model.add(observation, RDFS.subPropertyOf,SDMXDIMENSION.refPeriod);
			//					//						if()
			//					//						model.add(dim, RDFS.range,XmlSchema.gYear);
			//					return;
			//				}
			//				case "compound":return;
			//				case "measure":return;
			//				case "attribute":return;
			//			}
			
			if(currency!=null)
			{
				model.add(observation, DBO.currency, currency);				
			}

			if(yearLiteral!=null&&!dateExists) // fallback, in case entry doesnt have a date attached we use year of the whole dataset
			{				
				model.addLiteral(observation,LSO.refYear,yearLiteral);				
			}
			for(Resource country: countries)
			{
				// add the countries to the observations as well (not just the dataset)
				model.add(observation,SDMXATTRIBUTE.refArea,country);
			}
			if(model.size()>MAX_MODEL_TRIPLES)
			{
				log.fine("writing triples");
				writeModel(model,out);				
			}
		}
		// completeness statistics
		model.addLiteral(dataSet, LSO.completeness, 1-(double)(missingValues/expectedValues));
		for(ComponentProperty d: componentProperties)
		{
			model.addLiteral(d.property, LSO.completeness, 1-(double)(missingValues/expectedValues));
		}
		
		// in case the dataset goes over several years or doesnt have a default time attached we want all the years of the observations on the dataset  
		for(int year: years)
		{
			model.addLiteral(dataSet,LSO.refYear,model.createTypedLiteral(year, XSD.gYear.getURI()));
		}
		writeModel(model,out);
		// write missing statistics
		try(PrintWriter statisticsOut  = new PrintWriter(new BufferedWriter(new FileWriter(statistics, true))))
		{statisticsOut.println(datasetName+'\t'+((double)missingValues/i)+'\t'+(double)Collections.max(missingForProperty.values())/i);}
		
	}

	static void writeModel(Model model, OutputStream out)
	{		
		model.write(out,"N-TRIPLE");
		//		model.write(out,"TURTLE");
		// assuming that most memory is consumed before model cleaning
		memoryBenchmark.updateAndGetMaxMemoryBytes();
		model.removeAll();
	}

	/** Takes a json url of an openspending dataset model and extracts rdf into a jena model.  
	 * The DataStructureDefinition (DSD) specifies the structure of a dataset and contains a set of qb:ComponentSpecification resources.
	 * @param url json url that contains an openspending dataset model, e.g. http://openspending.org/fukuoka_2013/model  
	 * @param model initialized model that the triples will be added to
	 */
	static Resource createDataStructureDefinition(final URL url,Model model) throws MalformedURLException, IOException
	{		
		log.finer("Creating DSD");
		Resource dsd = model.createResource(url.toString());
		model.add(dsd, RDF.type, QB.DataStructureDefinition);
		//		JsonNode dsdJson = readJSON(url);
		// mapping is now gotten in createdataset
		//		JsonNode mapping = dsdJson.get("mapping");
		//		for(Iterator<String> it = mapping.keys();it.hasNext();)
		//		{
		//			String key = it.next();
		//			JsonNode dimJson = mapping.get(key);
		//			String type = dimJson.get("type");
		//			switch(type)
		//			{
		//				case "compound":return;
		//				case "measure":return;
		//				case "date":return;
		//				case "attribute":return;
		//			}

		//			if(1==1)throw new RuntimeException(dimURL);
		//			Resource dim = model.createResource(dimURL);
		//			model.add(dim,RDF.type,QB.DimensionProperty);

		//			String label = dimJson.get("label");
		//			if(label!=null&&!label.equals("null")) {model.add(dim,RDFS.label,label);}
		//			String description = dimJson.get("description");
		//			if(description!=null&&!description.equals("null")) {model.add(dim,RDFS.comment,description);}


		//			System.out.println(dimJson);
		//		}

		//		if(dsdJson.has("views"))
		//		{
		//			ArrayNode views = dsdJson.getArrayNode("views");	
		//		}

		//		System.out.println("Converting dataset "+url);
		return dsd;
	}

	static void deleteDataset(String datasetName)
	{
		System.out.println("******************************++deelte"+datasetName);
		getDatasetFile(datasetName).delete();
	}

	/** Takes the url of an openspending dataset and extracts rdf into a jena model.
	 * Each dataset contains a model which gets translated to a datastructure definition and entries that contain the actual measurements and get translated to a
	 * DataCube. 
	 * @param url json url that contains an openspending dataset, e.g. http://openspending.org/fukuoka_2013
	 * @param model initialized model that the triples will be added to
	 * @throws IOException 
	 * @throws NoCurrencyFoundForCodeException 
	 * @throws DatasetHasNoCurrencyException 
	 * @throws UnknownMappingTypeException 
	 * @throws TooManyMissingValuesException 
	 * @returns if it was successfully created 
	 */
	static void createDataset(String datasetName,Model model,OutputStream out)
			throws IOException, NoCurrencyFoundForCodeException, DatasetHasNoCurrencyException, MissingDataException, UnknownMappingTypeException, TooManyMissingValuesException		
			{
		@NonNull URL url = new URL(LS+datasetName);
		@NonNull URL sourceUrl = new URL(OS+datasetName+".json");		
		@NonNull JsonNode datasetJson = readJSON(sourceUrl);		
		@NonNull Resource dataSet = model.createResource(url.toString());		
		@NonNull Resource dsd = createDataStructureDefinition(new URL(url+"/model"), model);		
		model.add(dataSet,DCMI.source,model.createResource(OS+datasetName));
		model.add(dataSet,DCMI.created,model.createTypedLiteral(GregorianCalendar.getInstance()));

		// currency is defined on the dataset level in openspending but in RDF datacube we decided to define it for each observation 		
		Resource currency = null;

		if(datasetJson.has("currency"))
		{
			String currencyCode = datasetJson.get("currency").asText();
			currency = model.createResource(codeToCurrency.get(currencyCode));
			if(currency == null) {throw new NoCurrencyFoundForCodeException(datasetName,currencyCode);}
			model.add(dsd, QB.component, LSO.CurrencyComponentSpecification);			

			//			model.add(currencyComponent, QB.attribute, SDMXATTRIBUTE.currency);
			//			model.addLiteral(SDMXATTRIBUTE.currency, RDFS.label,model.createLiteral("currency"));
			//			model.add(SDMXATTRIBUTE.currency, RDF.type, RDF.Property);
			//			model.add(SDMXATTRIBUTE.currency, RDF.type, QB.AttributeProperty);
			//			//model.add(SDMXATTRIBUTE.currency, RDFS.subPropertyOf,SDMXMEASURE.obsValue);
			//			model.add(SDMXATTRIBUTE.currency, RDFS.range,XSD.decimal);
		} else {log.warning("no currency for dataset "+datasetName+", skipping");throw new DatasetHasNoCurrencyException(datasetName);}
		final Integer defaultYear;
		{
			String defaultYearString = cleanString(datasetJson.get("default_time").asText());
			// we only want the year, not date and time which are 1-1 and 0:0:0 anyways
			if(defaultYearString!=null) defaultYearString = defaultYearString.substring(0, 4);
			defaultYear = defaultYearString==null?null:Integer.valueOf(defaultYearString);
		}
		Set<ComponentProperty> componentProperties;
		try {componentProperties = createComponents(readJSON(new URL(OS+datasetName+"/model")).get("mapping"), model,datasetName,dataSet, dsd,defaultYear!=null);	}
		catch (MissingDataException | UnknownMappingTypeException e)
		{
			log.severe("Error creating components for dataset "+datasetName);
			throw e;
		}

		model.add(dataSet, RDF.type, QB.DataSet);
		model.add(dataSet, QB.structure, dsd);
		String dataSetName = url.toString().substring(url.toString().lastIndexOf('/')+1);

		List<String> territories = ArrayNodeToStringList((ArrayNode)datasetJson.get("territories"));
		Set<Resource> countries = new HashSet<>();
		@Nullable Literal yearLiteral = null; 
		if(defaultYear!=null)
		{
			model.add(dsd, QB.component, LSO.YearComponentSpecification);
			yearLiteral = model.createTypedLiteral(defaultYear, XSD.gYear.getURI());
			model.add(dataSet,LSO.refYear,yearLiteral);
		}
		if(!territories.isEmpty())
		{			
			model.add(dsd, QB.component, LSO.CountryComponent);
			for(String territory: territories)
			{
				Resource country = model.createResource(Countries.lgdCountryByCode.get(territory));
				countries.add(country);
				model.add(dataSet,SDMXATTRIBUTE.refArea,country);
			}
		}		
		{			
			//		JsonNode entries = readJSON(new URL("http://openspending.org/api/2/search?format=json&pagesize="+MAX_ENTRIES+"&dataset="+dataSetName),true);
			//		log.fine("extracting results");
			//		ArrayNode results = (ArrayNode)entries.get("results");
			log.fine("creating entries");

			if(conversionMode==SCHEMA_AND_OBSERVATIONS) createObservations(datasetName, model,out, dataSet,componentProperties,currency,countries,yearLiteral);
			log.fine("finished creating entries");
		}
		createViews(datasetName,model,dataSet);
		List<String> languages = ArrayNodeToStringList((ArrayNode)datasetJson.get("languages"));

		//		 qb:component [qb:attribute sdmx-attribute:unitMeasure; 
		//         qb:componentRequired "true"^^xsd:boolean;
		//         qb:componentAttachment qb:DataSet;] 
		String label = datasetJson.get("label").asText();
		String description = datasetJson.get("description").asText();

		// doesnt work well enough
		//		// guess the language for the language tag
		//		// we assume that label and description have the same language
		//		Detector detector = DetectorFactory.create();
		//		detector.append(label);
		//		detector.append(description);
		//		String language = detector.detect();
		//		model.add(dataSet, RDFS.label, label,language);
		//		model.add(dataSet, RDFS.comment, description,language);
		model.add(dataSet, RDFS.label, label);
		model.add(dataSet, RDFS.comment, description);
		// todo: find out the language
		//		model.createStatement(arg0, arg1, arg2)
		//		System.out.println("Converting dataset "+url);
			}

	/** @param sourceUrl	 e.g. http://openspending.org/cameroon_visualisation/views (.json will be added internally)*/
	public static void createViews(String datasetName,Model model, Resource dataSet) throws MalformedURLException, IOException
	{	
		ArrayNode views = readArrayNode(new URL(OS+datasetName+"/views.json"));
		for(int i=0;i<views.size();i++)
		{
			JsonNode jsonView = views.get(i);
			String name = jsonView.get("name").asText();
			Resource view = model.createResource(LS+datasetName+"/views/"+name);
			model.add(view,RDF.type,QB.Slice);
			model.add(dataSet,QB.slice,view);
			String label = jsonView.get("label").asText();
			String description = jsonView.get("description").asText();
			model.add(view, RDFS.label, label);
			model.add(view, RDFS.comment, description);
		}
	}

	public static String readJSONString(URL url) throws IOException {return readJSONString(url,false,USE_CACHE);}
	public static String readJSONString(URL url,boolean detailedLogging) throws IOException {return readJSONString(url,false,USE_CACHE);}
	public static String readJSONString(URL url,boolean detailedLogging,boolean USE_CACHE) throws IOException	
	{
		//		System.out.println(cache.getKeys());
		if(USE_CACHE)
		{
			Element e = cache.get(url.toString());
			if(e!=null) {/*System.out.println("cache hit for "+url.toString());*/return (String)e.getObjectValue();}
		}
		if(detailedLogging) {log.fine("cache miss for "+url.toString());}
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

	public static JsonNode readJSON(URL url,boolean detailedLogging) throws JsonProcessingException, IOException
	{
		String content = readJSONString(url,detailedLogging);
		if(detailedLogging) {log.fine("finished loading text, creating json object from text");}
		return m.readTree(content);
		//		try {return new JsonNode(readJSONString(url));}
		//		catch(JSONException e) {throw new IOException("Could not create a JSON object from string "+readJSONString(url),e);}
	}

	public static JsonNode readJSON(URL url) throws IOException
	{
		return readJSON(url,false);
	}

	public static ArrayNode readArrayNode(URL url) throws IOException	
	{
		return (ArrayNode)m.readTree(readJSONString(url));
		//		try {return new ArrayNode(readJSONString(url));}
		//		catch(JSONException e) {throw new IOException("Could not create a JSON array from string "+readJSONString(url),e);}
	}

	public static List<String> ArrayNodeToStringList(ArrayNode ja)
	{
		List<String> l = new LinkedList<>();
		for(int i=0;i<ja.size();i++)
		{
			l.add(ja.get(i).asText());
		}
		return l;
	}

	static Model newModel()
	{
		Model model = ModelFactory.createMemModelMaker().createDefaultModel();
		model.setNsPrefix("qb", "http://purl.org/linked-data/cube#");
		model.setNsPrefix("ls", LS);
		model.setNsPrefix("lso", LSO.URI);
		model.setNsPrefix("sdmx-subject",	"http://purl.org/linked-data/sdmx/2009/subject#");
		model.setNsPrefix("sdmx-dimension",	"http://purl.org/linked-data/sdmx/2009/dimension#");
		model.setNsPrefix("sdmx-attribute",	"http://purl.org/linked-data/sdmx/2009/attribute#");
		model.setNsPrefix("sdmx-measure",	"http://purl.org/linked-data/sdmx/2009/measure#");
		model.setNsPrefix("rdfs",RDFS.getURI());
		model.setNsPrefix("rdf",RDF.getURI());
		model.setNsPrefix("xsd",XSD.getURI());
		return model;
	}

	public static int nrEntries(String datasetName) throws MalformedURLException, IOException
	{
		return readJSON(new URL(OS+datasetName+"/entries.json?pagesize=0")).get("stats").get("results_count_query").asInt();		 
	}

	public static void main(String[] args) throws MalformedURLException, IOException
	{
		long startTime = System.currentTimeMillis();
		System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );		
		try{LogManager.getLogManager().readConfiguration();log.setLevel(Level.INFO);} catch ( RuntimeException e ) { e.printStackTrace();}
		try
		{			
			folder.mkdir();
			// observations use saved datasets so we need the saved names, if we only create the schema we can use the newest dataset names
			SortedSet<String> datasetNames = conversionMode==SCHEMA_AND_OBSERVATIONS?JsonDownloader.getSavedDatasetNames():JsonDownloader.getDatasetNames();
			// TODO: parallelize
			//		DetectorFactory.loadProfile("languageprofiles");			

			//			JsonNode datasets = m.readTree(new URL(DATASETS));			
			//			ArrayNode datasetArray = (ArrayNode)datasets.get("datasets");
			int exceptions = 0;
//			int notexisting = 0;
			int offset = 0;
			int i=0;
			int fileexists=0; 
			//			for(i=0;i<Math.min(datasetArray.length(),10);i++)
			//			for(i=5;i<=5;i++)
			for(final String datasetName : datasetNames)				
			{				
				//				if(!name.contains("orcamento_brasil_2000_2013")) continue;
//				if(!datasetName.contains("berlin_de")) continue;
				//				if(!datasetName.contains("2011saiki_budget")) continue;

				i++;				
				Model model = newModel();
//				Map<String,Property> componentPropertyByName = new HashMap<>();
				//				Map<String,Resource> hierarchyRootByName = new HashMap<>();
				//				Map<String,Resource> codeListByName = new HashMap<>();

				File file = getDatasetFile(datasetName);					
				if(file.exists()&&file.length()>0)
				{
					log.finer("skipping already existing file nr "+i+": "+file);
					fileexists++;
					continue;
				}
				try(OutputStream out = new FileOutputStream(file, true))
				{
					//					JsonNode dataSetJson = datasetArray.get(i);
					URL url = new URL(LS+datasetName);
					//					URL url = new URL(dataSetJson.get("html_url").asText());
					//					String name = dataSetJson.get("name").asText();
					//					int nrOfEntries = nrEntries(name);
					//					if(nrOfEntries==0)
					//					{
					//						log.warning("no entries found for dataset "+url);
					//						continue;
					//					}
					//					//		URL url = new URL("http://openspending.org/cameroon_visualisation");
					//					//								URL url = new URL("http://openspending.org/berlin_de");
					//					//		URL url = new URL("http://openspending.org/bmz-activities");
					//					log.info("Dataset nr. "+i+"/"+datasetArray.size()+": "+url);
					log.info("Dataset nr. "+i+"/"+datasetNames.size()+": "+url);										
					try
					{
						createDataset(datasetName,model,out);
						writeModel(model,out);
					}
					catch (NoCurrencyFoundForCodeException | DatasetHasNoCurrencyException | MissingDataException| UnknownMappingTypeException | TooManyMissingValuesException | FileNotFoundException e)
					{
						exceptions++;
						deleteDataset(datasetName);
						faultyDatasets.add(datasetName);
						log.severe("Error creating dataset "+datasetName+". Skipping.");
						e.printStackTrace();
						if(exceptions>=MIN_EXCEPTIONS_FOR_STOP&&((double)exceptions/(i+1))>EXCEPTION_STOP_RATIO	)
						{									
							log.severe("Too many exceptions ("+exceptions+" out of "+(i+1));
							shutdown(1);
						}

					}
					{
					}						
				}
			}
			//				catch(TooManyMissingValuesException e)
			//				{
			//					e.printStackTrace();
			//					log.severe(e.getLocalizedMessage());
			//					exceptions++;
			if(exceptions>=MIN_EXCEPTIONS_FOR_STOP&&((double)exceptions/(i+1))>EXCEPTION_STOP_RATIO	)
			{
				if(USE_CACHE) {cache.getCacheManager().shutdown();}
				log.severe("Too many exceptions ("+exceptions+" out of "+(i+1));
				shutdown(1);
			}
			//				}

//			log.info("Processed "+(i-offset)+" datasets with "+exceptions+" exceptions and "+notexisting+" not existing datasets, "+fileexists+" already existing ("+(i-exceptions-notexisting-fileexists)+" newly created).");
			log.info("** FINISHED CONVERSION: Processed "+(i-offset)+" datasets with "+exceptions+" exceptions and "+fileexists+" already existing ("+(i-exceptions-fileexists)+" newly created)."
					+"Processing time: "+(System.currentTimeMillis()-startTime)/1000+" seconds, maximum memory usage of "+memoryBenchmark.updateAndGetMaxMemoryBytes()/1000000+" MB.");
			if(faultyDatasets.size()>0) log.warning("Datasets with errors which were not converted: "+faultyDatasets);
		}

		// we must absolutely make sure that the cache is shut down before we leave the program, else cache can become corrupt which is a big time waster 
		catch(RuntimeException e) {log.severe(e.getLocalizedMessage());shutdown(1);}
		shutdown(0);
	}

	static void shutdown(int status)
	{
		if(USE_CACHE) {CacheManager.getInstance().shutdown();}
		System.exit(status);
	}

	static Map<String,File> files = new ConcurrentHashMap<String,File>();
	private static File getDatasetFile(String name)
	{
		File file = files.get(name);
		if(file==null) files.put(name,file= new File(folder+"/"+name+".nt"));
		return file;
	}
}