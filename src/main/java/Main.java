import java.io.File;
import java.io.IOException;
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
import java.util.logging.Level;
import java.util.logging.LogManager;
import lombok.extern.java.Log;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.cybozu.labs.langdetect.LangDetectException;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;

@NonNullByDefault
@Log
public class Main
{
	// TODO : use Jackson or gson
	static final boolean USE_CACHE = true;
	static final String DATASETS = "http://openspending.org/datasets.json";
	static final String OS = "http://openspending.org/";
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
		static final Property parentChildProperty = ResourceFactory.createProperty(qb+"parentChildProperty");;
	}

	static public class SDMXDIMENSION
	{
		static final String sdmxDimension = "http://purl.org/linked-data/sdmx/2009/dimension#";
		//		static final Property refPeriod = ResourceFactory.createProperty(sdmx+"refPeriod");
		static final Property refTime = ResourceFactory.createProperty(sdmxDimension+"refTime");		
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
	}

	static public class SDMXCONCEPT
	{
		static final String sdmxConcept = "http://purl.org/linked-data/sdmx/2009/concept#";
		static final Property obsValue = ResourceFactory.createProperty(sdmxConcept+"obsValue");		
		static final Property refPeriod = ResourceFactory.createProperty(sdmxConcept+"refPeriod");
		static final Property timePeriod = ResourceFactory.createProperty(sdmxConcept+"timePeriod");
	}

	static public class XmlSchema
	{
		static final String xmlSchema = "http://www.w3.org/2001/XMLSchema#";
		static final Property gYear = ResourceFactory.createProperty(xmlSchema+"gYear");
	}

	@Nullable static String cleanString(String s)
	{
		if("null".equals(s)) return null;
		return s;
	}

	/** Creates component specifications. Adds backlinks from their parent DataStructureDefinition.*/
	static Set<ComponentProperty> createComponents(JSONObject mapping, Model model,Resource dataset, Resource dsd,Map<String,Property> componentPropertyByName)
			throws MalformedURLException, IOException, JSONException
			{
		Set<ComponentProperty> componentProperties = new HashSet<>();
		//		JSONArray dimensionArray = readJSONArray(url);		

		for(Iterator<String> it = mapping.sortedKeys(); it.hasNext();)
		{
			//			JSONObject dimJson = dimensionArray.getJSONObject(i);
			String key = it.next();
			JSONObject componentJson = mapping.getJSONObject(key);

			//			String name = cleanString(componentJson.getString("name"));

			String name = key;
			String type = cleanString(componentJson.getString("type"));
			assert type!=null;
			String label = cleanString(componentJson.getString("label"));
			String description = cleanString(componentJson.getString("description"));

			//			String componentPropertyUrl = componentJson.getString("html_url");
			String componentPropertyUrl = dataset.getURI()+'/'+name;
			Resource componentSpecification = model.createResource(componentPropertyUrl+"/componentSpecification");//TODO: improve url
			Property componentProperty = model.createProperty(componentPropertyUrl);				
			componentPropertyByName.put(name, componentProperty);

			// backlink
			model.add(dsd, QB.component, componentSpecification);


			model.add(componentProperty, RDF.type, RDF.Property);

			if(label!=null) {model.add(componentProperty,RDFS.label,label);}
			else
			{
				label = name;
				if(label!=null) {model.add(componentProperty,RDFS.label,label);}
			}

			if(description!=null) {model.add(componentProperty,RDFS.comment,description);}							

			switch(type)
			{
				case "date":
				{
					// it's a dimension
					model.add(componentSpecification, QB.dimension, componentProperty);
					model.add(componentProperty, RDF.type, QB.DimensionProperty);

					model.add(componentProperty, RDFS.subPropertyOf,SDMXDIMENSION.refTime);
					componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.DATE));

					// concept
					model.add(componentProperty, QB.concept,SDMXCONCEPT.timePeriod);  
					//						if()
					//						model.add(dim, RDFS.range,XmlSchema.gYear);
					break;
				}
				case "compound":
				{
					// it's a dimension
					model.add(componentSpecification, QB.dimension, componentProperty);
					model.add(componentProperty, RDF.type, QB.DimensionProperty);
					//						assertTrue(); TODO: assert that the "attributes" of the json are always "name" and "label"
					componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.COMPOUND));
					//TODO: model.add(componentProperty, QB.concept,SDMXCONCEPT. ???); 
					break;
				}
				case "measure":
				{
					model.add(componentSpecification, QB.measure, componentProperty);
					model.add(componentProperty, RDF.type, QB.MeasureProperty);

					componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.MEASURE));
					//TODO: model.add(componentProperty, QB.concept,SDMXCONCEPT. ???);
					break;
				}
				case "attribute":
				{
					// TODO: attribute the same meaning as in DataCube?
					model.add(componentSpecification, QB.attribute, componentProperty);
					model.add(componentProperty, RDF.type, QB.AttributeProperty);

					componentProperties.add(new ComponentProperty(componentProperty,name,ComponentProperty.Type.ATTRIBUTE));
					//TODO: model.add(componentProperty, QB.concept,SDMXCONCEPT. ???);
					break;
				}
				default: throw new RuntimeException("unkown type: "+type+"of mapping element "+componentJson);
			}			
		}
		return componentProperties;
			}

	//	static void createViews(URL url, Model model) throws MalformedURLException, IOException, JSONException
	//	{
	//		JSONArray views = readJSONArray(new URL(url+".json"));
	//		for(int i=0;i<views.length();i++)
	//		{
	//			JSONObject view = views.getJSONObject(i);
	//
	//			String name = view.getString("name");
	//			String label = view.getString("label");
	//			String description = view.getString("description");
	//
	//			JSONObject state = view.getJSONObject("state");			
	//			String year = state.getString("year"); // TODO: what to do with the year?
	//			Integer.valueOf(year); // throws an exception if its not an integer
	//
	//			JSONObject cuts = state.getJSONObject("cuts");
	//			List<String> drilldowns = jsonArrayToStringList(state.getJSONArray("drilldowns"));
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
	//					String dimensionValue = cuts.getString(dimensionName);
	//					model.add(sliceKey,QB.componentProperty,componentPropertyByName.get(dimensionName));
	//					model.add(slice,componentPropertyByName.get(dimensionName),dimensionValue);
	//				}
	//
	//			}
	//
	//
	//		}
	//	}

	static void createCodeLists(JSONArray views, Model model) throws MalformedURLException, IOException, JSONException
	{
		for(int i=0;i<views.length();i++)
		{
		}
	}

	//	static void createViews(URL datasetUrl, JSONArray views, Model model,Map<String,Property> componentPropertyByName) throws MalformedURLException, IOException, JSONException
	//	{		
	//		for(int i=0;i<views.length();i++)
	//		{
	//			JSONObject view = views.getJSONObject(i);
	//			String entity = view.getString("entity");			
	//			String name = view.getString("name");
	//			String drilldownName = view.getString("drilldown");
	//			Property drilldownProperty = componentPropertyByName.get("drilldown");
	//
	//			String label = view.getString("label");
	//			String description = view.getString("description");
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
	//			JSONObject state = view.getJSONObject("state");			
	//			String year = state.getString("year"); // TODO: what to do with the year?
	//			Integer.valueOf(year); // throws an exception if its not an integer
	//
	//			JSONObject cuts = state.getJSONObject("cuts");
	//			List<String> drilldowns = jsonArrayToStringList(state.getJSONArray("drilldowns"));
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
	//			//					String dimensionValue = cuts.getString(dimensionName);
	//			//					model.add(sliceKey,QB.componentProperty,componentPropertyByName.get(dimensionName));
	//			//					model.add(slice,componentPropertyByName.get(dimensionName),dimensionValue);
	//			//				}
	//			//
	//			//			}
	//
	//
	//		}
	//	}
	/** @param url entries url, e.g. http://openspending.org/berlin_de/entries.json	 (TODO: or http://openspending.org/api/2/search?dataset=berlin_de&format=json ?) 
	 * @param componentProperties the dimensions which are expected to be values for in all entries. */
	static void createEntries(JSONArray results, Model model, Resource dataSet, Set<ComponentProperty> componentProperties,@Nullable  Literal currencyLiteral) throws MalformedURLException, IOException, JSONException
	{
		//		JSONObject entries = readJSON(url);		
		for(int i=0;i<results.length();i++)
		{
			JSONObject result = results.getJSONObject(i);
			Resource observation = model.createResource(result.getString("html_url"));				
			model.add(observation, QB.dataSet, dataSet);			
			model.add(observation, RDF.type, QB.Observation);

			for(ComponentProperty d: componentProperties)
			{
				try
				{
					switch(d.type)
					{
						case COMPOUND:
						{
							JSONObject jsonDim = result.getJSONObject(d.name);
							Resource instance = model.createResource(jsonDim.getString("html_url"));

							if(jsonDim.has("label")) {model.addLiteral(instance,RDFS.label,model.createLiteral(jsonDim.getString("label")));}
							else	{log.warning("no label for dimension "+d.name+" instance "+instance);}
							model.add(observation,d.property,instance);

							break;
						}
						case ATTRIBUTE:
						{
							String s = result.getString(d.name);
							model.addLiteral(observation,d.property,model.createLiteral(s));			
							break;
						}
						case MEASURE:
						{
							String s = result.getString(d.name);
							model.addLiteral(observation,d.property,model.createLiteral(s));			

							break;
						}
						case DATE:
						{
							JSONObject jsonDate = result.getJSONObject(d.name);
							//							String week = date.getString("week");
							String year = jsonDate.getString("year");
							String month = jsonDate.getString("month");
							String day = jsonDate.getString("day");							
							model.addLiteral(observation,d.property,model.createTypedLiteral(year+"-"+month+"-"+day, XSD.date.getURI()));							
						}
					}

				}
				catch(Exception e)
				{
					if(USE_CACHE) {cache.getCacheManager().shutdown();}
					throw new RuntimeException("problem with componentproperty "+d.name+": "+observation,e);
				} 
			}
			//			
			//			String label = result.getString("label");
			//
			//			if(label!=null&&!label.equals("null")) {model.add(observation,RDFS.label,label);}
			//			else
			//			{
			//				label = result.getString("name");
			//				if(label!=null&&!label.equals("null")) {model.add(observation,RDFS.label,label);}
			//			}
			//			String description = result.getString("description");
			//			if(description!=null&&!description.equals("null")) {model.add(observation,RDFS.comment,description);}				
			//
			//			String type = result.getString("type");
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
		}			
	}

	/** Takes a json url of an openspending dataset model and extracts rdf into a jena model.  
	 * The DataStructureDefinition (DSD) specifies the structure of a dataset and contains a set of qb:ComponentSpecification resources.
	 * @param url json url that contains an openspending dataset model, e.g. http://openspending.org/fukuoka_2013/model  
	 * @param model initialized model that the triples will be added to
	 */
	static Resource createDataStructureDefinition(final URL url,Model model) throws MalformedURLException, IOException, JSONException
	{		
		log.finer("Creating DSD");
		Resource dsd = model.createResource(url.toString());
		model.add(dsd, RDF.type, QB.DataStructureDefinition);
		JSONObject dsdJson = readJSON(url);
		// mapping is now gotten in createdataset
		//		JSONObject mapping = dsdJson.getJSONObject("mapping");
		//		for(Iterator<String> it = mapping.keys();it.hasNext();)
		//		{
		//			String key = it.next();
		//			JSONObject dimJson = mapping.getJSONObject(key);
		//			String type = dimJson.getString("type");
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

		//			String label = dimJson.getString("label");
		//			if(label!=null&&!label.equals("null")) {model.add(dim,RDFS.label,label);}
		//			String description = dimJson.getString("description");
		//			if(description!=null&&!description.equals("null")) {model.add(dim,RDFS.comment,description);}


		//			System.out.println(dimJson);
		//		}

		//		if(dsdJson.has("views"))
		//		{
		//			JSONArray views = dsdJson.getJSONArray("views");	
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
	static void createDataset(URL url,Model model,Map<String,Property> componentPropertyByName) throws JSONException, IOException, LangDetectException
	{				
		JSONObject datasetJson = readJSON(new URL(url.toString()+".json"));		
		Resource dataSet = model.createResource(url.toString());	
		Resource dsd = createDataStructureDefinition(new URL(url+"/model"), model);
		// currency is defined on the dataset level in openspending but in RDF datacube we decided to define it for each observation 		
		Literal currencyLiteral = null;

		if(datasetJson.has("currency"))
		{
			String currency = datasetJson.getString("currency");
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

		Set<ComponentProperty> componentProperties = createComponents(readJSON(new URL(url+"/model")).getJSONObject("mapping"), model,dataSet, dsd,componentPropertyByName);

		model.add(dataSet, RDF.type, QB.DataSet);
		model.add(dataSet, QB.structure, dsd);
		String dataSetName = url.toString().substring(url.toString().lastIndexOf('/')+1);
		
		{
		log.fine("loading entries");
		JSONObject entries = readJSON(new URL("http://openspending.org/api/2/search?format=json&pagesize="+MAX_ENTRIES+"&dataset="+dataSetName));
		log.fine("extracting results");
		JSONArray results = entries.getJSONArray("results");
		log.fine("finished loading entries, creating entries");
		createEntries(results, model, dataSet,componentProperties,currencyLiteral);
		log.fine("finished creating entries");
		}
		createViews(new URL(url+"/views"),model,dataSet);
		List<String> languages = jsonArrayToStringList(datasetJson.getJSONArray("languages"));
		List<String> territories = jsonArrayToStringList(datasetJson.getJSONArray("territories"));		

		String label = datasetJson.getString("label");
		String description = datasetJson.getString("description");

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

	/** @param url	 e.g. http://openspending.org/cameroon_visualisation/views (.json will be added internally)*/
	public static void createViews(URL url,Model model, Resource dataSet) throws MalformedURLException, IOException, JSONException
	{	
		JSONArray views = readJSONArray(new URL(url+".json"));
		for(int i=0;i<views.length();i++)
		{
			JSONObject jsonView = views.getJSONObject(i);
			String name = jsonView.getString("name");
			Resource view = model.createResource(url+"/"+name);
			model.add(view,RDF.type,QB.Slice);
			model.add(dataSet,QB.slice,view);
			String label = jsonView.getString("label");
			String description = jsonView.getString("description");
			model.add(view, RDFS.label, label);
			model.add(view, RDFS.comment, description);
		}
	}

	public static String readJSONString(URL url) throws MalformedURLException, IOException, JSONException	
	{
		//		System.out.println(cache.getKeys());
		if(USE_CACHE)
		{
			Element e = cache.get(url.toString());
			if(e!=null) {/*System.out.println("cache hit for "+url.toString());*/return (String)e.getObjectValue();}
		}
		//		System.out.println("cache miss for "+url.toString());
		try(Scanner undelimited = new Scanner(url.openStream(), "UTF-8"))
		{
			try(Scanner scanner = undelimited.useDelimiter("\\A"))
			{
				String datasetsJsonString = scanner.next();
				char firstChar = datasetsJsonString.charAt(0);
				if(!(firstChar=='{'||firstChar=='[')) {throw new JSONException("JSON String for URL "+url+" seems to be invalid.");}
				if(USE_CACHE) {cache.put(new Element(url.toString(), datasetsJsonString));}
				//IfAbsent			
				return datasetsJsonString;			
			}
		}
	}

	public static JSONObject readJSON(URL url) throws MalformedURLException, IOException, JSONException	
	{
		try {return new JSONObject(readJSONString(url));}
		catch(JSONException e) {throw new IOException("Could not create a JSON object from string "+readJSONString(url),e);}
	}

	public static JSONArray readJSONArray(URL url) throws MalformedURLException, IOException, JSONException	
	{		
		try {return new JSONArray(readJSONString(url));}
		catch(JSONException e) {throw new IOException("Could not create a JSON array from string "+readJSONString(url),e);}
	}

	public static List<String> jsonArrayToStringList(JSONArray ja) throws JSONException	
	{
		List<String> l = new LinkedList<>();
		for(int i=0;i<ja.length();i++)
		{
			l.add(ja.getString(i));
		}
		return l;
	}

	static Model newModel()
	{
		Model model = ModelFactory.createMemModelMaker().createDefaultModel();
		model.setNsPrefix("qb", "http://purl.org/linked-data/cube#");
		model.setNsPrefix("os", OS);
		model.setNsPrefix("sdmx-subject",	"http://purl.org/linked-data/sdmx/2009/subject#");
		model.setNsPrefix("sdmx-dimension",	"http://purl.org/linked-data/sdmx/2009/dimension#");
		model.setNsPrefix("sdmx-attribute",	"http://purl.org/linked-data/sdmx/2009/attribute#");
		model.setNsPrefix("sdmx-measure",	"http://purl.org/linked-data/sdmx/2009/measure#");
		model.setNsPrefix("rdfs",RDFS.getURI());
		model.setNsPrefix("rdf",RDF.getURI());
		model.setNsPrefix("xsd",XSD.getURI());
		return model;
	}

	public static boolean exists(URL dataset) throws MalformedURLException, JSONException, IOException
	{
		return !readJSON(new URL(dataset+"/entries.json?pagesize=0")).getJSONObject("stats").getString("results_count_query").equals("0");		 
	}

	public static void main(String[] args) throws MalformedURLException, IOException, JSONException, LangDetectException, Exception
	{
		System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
		try{LogManager.getLogManager().readConfiguration();log.setLevel(Level.FINE);} catch ( Exception e ) { e.printStackTrace();}
		
		try
		{
			// TODO: parallelize
			//		DetectorFactory.loadProfile("languageprofiles");			
			JSONObject datasets = readJSON(new URL(DATASETS));
			JSONArray datasetArray =  datasets.getJSONArray("datasets");
			int exceptions = 0;
			int notexisting = 0;
			int i;
			//			for(i=0;i<Math.min(datasetArray.length(),10);i++)
			for(i=129;i<datasetArray.length();i++)
			{
				Model model = newModel();
				Map<String,Property> componentPropertyByName = new HashMap<>();
				//				Map<String,Resource> hierarchyRootByName = new HashMap<>();
				//				Map<String,Resource> codeListByName = new HashMap<>();
				try
				{
					JSONObject dataSetJson = datasetArray.getJSONObject(i);
					URL url = new URL(dataSetJson.getString("html_url"));
					if(!exists(url))
					{
						log.warning("no entries found for dataset "+url);
						continue;
					}
					//		URL url = new URL("http://openspending.org/cameroon_visualisation");
					//								URL url = new URL("http://openspending.org/berlin_de");
					//		URL url = new URL("http://openspending.org/bmz-activities");
					log.info("Dataset nr. "+i+"/"+datasetArray.length()+": "+url);
					createDataset(url,model,componentPropertyByName);

					//			if(i>0) break;
					File folder = new File("output");
					folder.mkdir();		
					model.write(new PrintWriter(folder+"/"+url.toString().substring(url.toString().lastIndexOf('/')+1)+".ttl"),"TURTLE",null);
				}			
				catch(Exception e)
				{
					log.severe(e.getLocalizedMessage());
					exceptions++;
					if(exceptions>=MIN_EXCEPTIONS_FOR_STOP&&((double)exceptions/(i+1))>EXCEPTION_STOP_RATIO	)
					{throw new Exception("Too many exceptions ("+exceptions+" out of "+(i+1),e);}
				}
			}
			log.info("Processed "+i+"datasets with "+exceptions+" exceptions and "+notexisting+" not existing datasets ("+(i-exceptions-notexisting)+"remaining).");
		}

		// we must absolutely make sure that the cache is shut down before we leave the program, else cache can become corrupt which is a big time waster 
		catch(Exception e) {log.severe(e.getLocalizedMessage());CacheManager.getInstance().shutdown();throw e;}
		CacheManager.getInstance().shutdown();
	}
}