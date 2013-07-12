import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.logging.Level;
import java.util.logging.LogManager;
import lombok.extern.java.Log;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import com.cybozu.labs.langdetect.LangDetectException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;

@NonNullByDefault
@Log
public class Main
{
	static ObjectMapper m = new ObjectMapper();
	static final int MAX_MODEL_TRIPLES = 100_000;
	static final boolean USE_CACHE = true;
	public static final String DATASETS = "http://openspending.org/datasets.json";	
	static final String LS = "http://linkedspending.aksw.org/";
	static final String OS = "http://openspending.org/";
	static File folder = new File("output4");	 
	//	static final boolean CACHING = true;
	static {
		if(USE_CACHE) {CacheManager.getInstance().addCacheIfAbsent("openspending-json");}
	}
	static final Cache cache = USE_CACHE?CacheManager.getInstance().getCache("openspending-json"):null;
	private static final int	MAX_ENTRIES	= Integer.MAX_VALUE;
	//	private static final int	MAX_ENTRIES	= 30;
	private static final int	MIN_EXCEPTIONS_FOR_STOP	= 5;
	private static final float	EXCEPTION_STOP_RATIO	= 0.3f;

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

	static public class SDMXDIMENSION
	{
		static final String sdmxDimension = "http://purl.org/linked-data/sdmx/2009/dimension#";
		static final Property refPeriod = ResourceFactory.createProperty(sdmxDimension+"refPeriod");
//		static final Property timePeriod = ResourceFactory.createProperty(sdmxDimension+"timePeriod");
	}

	static public class SDMXMEASURE
	{
		static final String sdmxMeasure = "http://purl.org/linked-data/sdmx/2009/measure#";
		static final Property obsValue = ResourceFactory.createProperty(sdmxMeasure+"obsValue");		
	}

	static public class SDMXATTRIBUTE
	{
		static final String sdmxAttribute = "http://purl.org/linked-data/sdmx/2009/attribute#";
		static final Property currency = ResourceFactory.createProperty(sdmxAttribute+"currency");
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

	static public class LinkedSpendingOntology
	{
		static final String linkedSpendingOntology = LS+"ontology/";
		static public Resource CountryComponent = ResourceFactory.createResource(linkedSpendingOntology+"CountryComponentSpecification");
		static public Resource TimeComponentSpecification = ResourceFactory.createResource(linkedSpendingOntology+"CountryComponentSpecification");
	}

	@Nullable static String cleanString(String s)
	{
		if("null".equals(s)) return null;
		return s;
	}

	/** Creates component specifications. Adds backlinks from their parent DataStructureDefinition.*/
	static Set<ComponentProperty> createComponents(JsonNode mapping, Model model,Resource dataset, Resource dsd,Map<String,Property> componentPropertyByName)
			throws MalformedURLException, IOException
			{
		Set<ComponentProperty> componentProperties = new HashSet<>();
		//		ArrayNode dimensionArray = readArrayNode(url);		

		for(Iterator<String> it = mapping.fieldNames();it.hasNext();)
		{
			//			JsonNode dimJson = dimensionArray.get(i);
			String key = it.next();
			JsonNode componentJson = mapping.get(key);

			//			String name = cleanString(componentJson.get("name"));

			String name = key;
			String type = cleanString(componentJson.get("type").asText());
			assert type!=null;
			String label = cleanString(componentJson.get("label").asText());

			//			String componentPropertyUrl = componentJson.get("html_url");
			String componentPropertyUrl = dataset.getURI()+'/'+name;			

			Property componentProperty = model.createProperty(componentPropertyUrl);				
			componentPropertyByName.put(name, componentProperty);

			Resource componentSpecification = model.createResource(componentPropertyUrl+"/componentSpecification");//TODO: improve url			

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
					componentSpecification = LinkedSpendingOntology.TimeComponentSpecification;
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
					model.add(componentSpecification, QB.measure, componentProperty);
					model.add(componentSpecification, RDF.type, QB.ComponentSpecification);
					model.add(componentProperty, RDF.type, QB.MeasureProperty);

					componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.MEASURE));
					//TODO: model.add(componentProperty, QB.concept,SDMXCONCEPT. ???);
					break;
				}
				case "attribute":
				{
					// TODO: attribute the same meaning as in DataCube?
					model.add(componentSpecification, QB.attribute, componentProperty);
					model.add(componentSpecification, RDF.type, QB.ComponentSpecification);
					model.add(componentProperty, RDF.type, QB.AttributeProperty);

					componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.ATTRIBUTE));
					//TODO: model.add(componentProperty, QB.concept,SDMXCONCEPT. ???);
					break;
				}
				default: throw new RuntimeException("unkown type: "+type+"of mapping element "+componentJson);
			}
			// backlink
			model.add(dsd, QB.component, componentSpecification);
		}
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
	/**deletes the model! todo: check if ttl is not a problem. @param url entries url, e.g. http://openspending.org/berlin_de/entries.json	 (TODO: or http://openspending.org/api/2/search?dataset=berlin_de&format=json ?) 
	 * @param componentProperties the dimensions which are expected to be values for in all entries. */
	
	static void createEntries(String datasetName,Model model,OutputStream out, Resource dataSet, Set<ComponentProperty> componentProperties,@Nullable  Literal currencyLiteral) throws MalformedURLException, IOException
	{
		JsonDownloader.ResultsReader in = new JsonDownloader.ResultsReader(datasetName);
		JsonNode result;
		int errors=0;
		for(int i=0;(result=in.read())!=null;i++)		
		{			
			Resource observation = model.createResource(result.get("html_url").asText());				
			model.add(observation, QB.dataSet, dataSet);			
			model.add(observation, RDF.type, QB.Observation);

			for(ComponentProperty d: componentProperties)
			{
				//				if(d.name==null) {throw new RuntimeException("no name for component property "+d);}
				if(!result.has(d.name))
				{
					log.warning("no entry for property "+d.name+" at entry "+result);
					errors++;
					if(errors>10) throw new RuntimeException("too many errors");
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
								errors++;
								continue;
							}
							JsonNode urlNode = jsonDim.get("html_url");
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
							model.addLiteral(observation,d.property,model.createLiteral(s));			

							break;
						}
						case DATE:
						{
							JsonNode jsonDate = result.get(d.name);
							//							String week = date.get("week");
							String year = jsonDate.get("year").asText();
							String month = jsonDate.get("month").asText();
							String day = jsonDate.get("day").asText();							
							model.addLiteral(observation,SDMXDIMENSION.refPeriod,model.createTypedLiteral(year+"-"+month+"-"+day, XSD.date.getURI()));							
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
			if(currencyLiteral!=null)
			{				
				model.addLiteral(observation, SDMXATTRIBUTE.currency, currencyLiteral);				
			}
			if(model.size()>MAX_MODEL_TRIPLES)
			{
				log.fine("writing triples");
				writeModel(model,out);				
			}
		}
		writeModel(model,out);
	}
	
	static void writeModel(Model model, OutputStream out)
	{
		model.write(out,"TURTLE");
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

	/** Takes the url of an openspending dataset and extracts rdf into a jena model.
	 * Each dataset contains a model which gets translated to a datastructure definition and entries that contain the actual measurements and get translated to a
	 * DataCube. 
	 * @param url json url that contains an openspending dataset, e.g. http://openspending.org/fukuoka_2013
	 * @param model initialized model that the triples will be added to
	 */
	static void createDataset(String datasetName,Model model,OutputStream out,Map<String,Property> componentPropertyByName) throws IOException, LangDetectException
	{
		URL url = new URL(LS+datasetName);
		URL sourceUrl = new URL(OS+datasetName+".json");
		JsonNode datasetJson = readJSON(sourceUrl);		
		Resource dataSet = model.createResource(url.toString());	
		Resource dsd = createDataStructureDefinition(new URL(url+"/model"), model);
		// currency is defined on the dataset level in openspending but in RDF datacube we decided to define it for each observation 		
		Literal currencyLiteral = null;

		if(datasetJson.has("currency"))
		{
			String currency = datasetJson.get("currency").asText();
			currencyLiteral = model.createLiteral(currency);
			Resource currencyComponent = model.createResource(dataSet.getURI()+"/component/currency");
			model.add(dsd, QB.component, currencyComponent);			

			model.add(currencyComponent, QB.attribute, SDMXATTRIBUTE.currency);
			model.addLiteral(SDMXATTRIBUTE.currency, RDFS.label,model.createLiteral("currency"));
			model.add(SDMXATTRIBUTE.currency, RDF.type, RDF.Property);
			model.add(SDMXATTRIBUTE.currency, RDF.type, QB.AttributeProperty);
			//model.add(SDMXATTRIBUTE.currency, RDFS.subPropertyOf,SDMXMEASURE.obsValue);
			model.add(SDMXATTRIBUTE.currency, RDFS.range,XSD.decimal);

		}

		Set<ComponentProperty> componentProperties = createComponents(readJSON(new URL(OS+datasetName+"/model")).get("mapping"), model,dataSet, dsd,componentPropertyByName);

		model.add(dataSet, RDF.type, QB.DataSet);
		model.add(dataSet, QB.structure, dsd);
		String dataSetName = url.toString().substring(url.toString().lastIndexOf('/')+1);

		{			
			//		JsonNode entries = readJSON(new URL("http://openspending.org/api/2/search?format=json&pagesize="+MAX_ENTRIES+"&dataset="+dataSetName),true);
			//		log.fine("extracting results");
			//		ArrayNode results = (ArrayNode)entries.get("results");
			log.fine("creating entries");			
			createEntries(datasetName, model,out, dataSet,componentProperties,currencyLiteral);
			log.fine("finished creating entries");
		}
		createViews(datasetName,model,dataSet);
		List<String> languages = ArrayNodeToStringList((ArrayNode)datasetJson.get("languages"));
		List<String> territories = ArrayNodeToStringList((ArrayNode)datasetJson.get("territories"));
		if(!territories.isEmpty())
		{			
			model.add(dsd, QB.component, LinkedSpendingOntology.CountryComponent);
			for(String territory: territories)
			{
				Resource country = model.createResource(Countries.lgdCountryByCode.get(territory));
				model.add(dataSet,SDMXATTRIBUTE.refArea,country);
			}
		}
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

	public static void main(String[] args) throws MalformedURLException, IOException, LangDetectException, Exception
	{
		System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
		try{LogManager.getLogManager().readConfiguration();log.setLevel(Level.FINE);} catch ( Exception e ) { e.printStackTrace();}
		try
		{			
			folder.mkdir();
			SortedSet<String> datasetNames = JsonDownloader.getSavedDatasetNames();
			// TODO: parallelize
			//		DetectorFactory.loadProfile("languageprofiles");			

			//			JsonNode datasets = m.readTree(new URL(DATASETS));			
			//			ArrayNode datasetArray = (ArrayNode)datasets.get("datasets");

			int exceptions = 0;
			int notexisting = 0;
			int offset = 0;
			int i=0;
			int fileexists=0; 
			//			for(i=0;i<Math.min(datasetArray.length(),10);i++)
			//			for(i=5;i<=5;i++)
			for(String name : datasetNames)				
				//			for(i=offset;i<datasetArray.size();i++)
			{				
				if(!name.contains("orcamento_brasil_2000_2013")) continue;
				i++;				
				Model model = newModel();
				Map<String,Property> componentPropertyByName = new HashMap<>();
				//				Map<String,Resource> hierarchyRootByName = new HashMap<>();
				//				Map<String,Resource> codeListByName = new HashMap<>();
				try
				{					
					File file = new File(folder+"/"+name+".ttl");					
					if(file.exists())
					{
						log.finer("skipping already existing file nr "+i+": "+file);
						fileexists++;
						continue;
					}
					try(OutputStream out = new FileOutputStream(file, true))
					{
					//					JsonNode dataSetJson = datasetArray.get(i);
					URL url = new URL(LS+name);
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
					createDataset(name,model,out,componentPropertyByName);
					////										
					writeModel(model,out);
					}
				}			
				catch(Exception e)
				{
					e.printStackTrace();
					log.severe(e.getLocalizedMessage());
					exceptions++;
					if(exceptions>=MIN_EXCEPTIONS_FOR_STOP&&((double)exceptions/(i+1))>EXCEPTION_STOP_RATIO	)
					{
						if(USE_CACHE) {cache.getCacheManager().shutdown();}
						throw new Exception("Too many exceptions ("+exceptions+" out of "+(i+1),e);
					}
				}
			}
			log.info("Processed "+(i-offset)+" datasets with "+exceptions+" exceptions and "+notexisting+" not existing datasets, "+fileexists+" already existing ("+(i-exceptions-notexisting-fileexists)+" newly created).");
		}

		// we must absolutely make sure that the cache is shut down before we leave the program, else cache can become corrupt which is a big time waster 
		catch(Exception e) {log.severe(e.getLocalizedMessage());CacheManager.getInstance().shutdown();throw e;}
		CacheManager.getInstance().shutdown();
	}
}