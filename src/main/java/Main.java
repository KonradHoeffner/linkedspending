import java.io.File;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.cybozu.labs.langdetect.LangDetectException;
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
public class Main
{
	static final String DATASETS = "http://openspending.org/datasets.json";
	static final String OS = "http://openspending.org/";
	//	static final boolean CACHING = true;
	static {
		CacheManager.getInstance().addCacheIfAbsent("openspending-json");
	}
	static final Cache cache = CacheManager.getInstance().getCache("openspending-json");

	static public class QB
	{
		static final String qb = "http://purl.org/linked-data/cube#";
		static final Resource DataStructureDefinition = ResourceFactory.createResource(qb+"DataStructureDefinition");
		static final Resource DataSet = ResourceFactory.createResource(qb+"DataSet");
		static final Property dataSet = ResourceFactory.createProperty(qb+"dataSet");
		static final Property component = ResourceFactory.createProperty(qb+"component");
		static final Resource DimensionProperty = ResourceFactory.createResource(qb+"DimensionProperty");
		static final Resource MeasureProperty = ResourceFactory.createResource(qb+"MeasureProperty");
		static final Resource AttributeProperty = ResourceFactory.createResource(qb+"MeasureProperty");
		
		static final Property structure = ResourceFactory.createProperty(qb+"structure");
		static final Property componentProperty = ResourceFactory.createProperty(qb+"componentProperty");
		static final Property dimension = ResourceFactory.createProperty(qb+"dimension");
		static final Property measure = ResourceFactory.createProperty(qb+"measure");
		static final Property attribute = ResourceFactory.createProperty(qb+"attribute");
		static final Property concept = ResourceFactory.createProperty(qb+"concept");
		static final Resource Observation	= ResourceFactory.createResource(qb+"Observation");
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
	
	static public class SDMXCONCEPT
	{
		static final String sdmxConcept = "http://purl.org/linked-data/sdmx/2009/concept#";
		static final Property obsValue = ResourceFactory.createProperty(sdmxConcept+"obsValue");		
		static final Property refPeriod = ResourceFactory.createProperty(sdmxConcept+"refPeriod");
		static final Property timePeriod = ResourceFactory.createProperty(sdmxConcept+"timePeriod");
	}
	
	static public class XmlSchema
	{
		static final String xmlSchema = "http://purl.org/linked-data/sdmx/2009/dimension#";
		static final Property gYear = ResourceFactory.createProperty(xmlSchema+"gYear");
	}	

static void createViews()
{
	// TODO: implement
}

@Nullable static String cleanString(String s)
{
	if("null".equals(s)) return null;
	return s;
}
	
	/** Creates component specifications. Adds backlinks from their parent DataStructureDefinition.
	 * @param currency 3 character currency code, may be null*/
	static Set<ComponentProperty> createComponents(JSONObject mapping, Model model,Resource dataset, Resource dsd,@Nullable String currency) throws MalformedURLException, IOException, JSONException
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
		if(currency!=null)
		{
//			Resource amountComponent = model.createResource(url.toString().replace("/dimensions.json","")+"/amount");
//			model.add(dsd, QB.component, amountComponent);
//			
//			Resource amountMeasure = model.createResource(url.toString().replace("/dimensions.json","")+"/amount");
//			model.add(amountComponent, QB.measure, amountMeasure);
//			model.addLiteral(amountMeasure, RDFS.label,model.createLiteral("amount","en"));
//			model.add(amountMeasure, RDF.type, RDF.Property);
//			model.add(amountMeasure, RDF.type, QB.MeasureProperty);
//			model.add(amountMeasure, RDFS.subPropertyOf,SDMXMEASURE.obsValue);
//			model.add(amountMeasure, RDFS.range,XSD.decimal);

			Resource currencyComponent = model.createResource(dataset.getURI()+"/component/currency");
			model.add(dsd, QB.component, currencyComponent);
			Property currencyAttribute = model.createProperty(dataset.getURI()+"/attribute/currency");
			model.add(currencyComponent, QB.attribute, currencyAttribute);
			
			model.addLiteral(currencyAttribute, RDFS.label,model.createLiteral("currency"));
			model.add(currencyAttribute, RDF.type, RDF.Property);
			model.add(currencyAttribute, RDF.type, QB.MeasureProperty);
			model.add(currencyAttribute, RDFS.subPropertyOf,SDMXMEASURE.obsValue);
			model.add(currencyAttribute, RDFS.range,XSD.decimal);

		}
		return componentProperties;
	}

	/** @param url entries url, e.g. http://openspending.org/berlin_de/entries.json	 (TODO: or http://openspending.org/api/2/search?dataset=berlin_de&format=json ?) 
	 * @param componentProperties the dimensions which are expected to be values for in all entries. */
	static void createEntries(URL url, Model model, Resource dataSet, Set<ComponentProperty> componentProperties) throws MalformedURLException, IOException, JSONException
	{
		JSONObject entries = readJSON(url);
		JSONArray results = entries.getJSONArray("results");
		for(int i=0;i<results.length();i++)
		{
			JSONObject result = results.getJSONObject(i);
			Resource observation = model.createResource(result.getString("html_url"));				
			model.add(observation, QB.dataSet, dataSet);			
			model.add(observation, RDF.type, QB.Observation);

			for(ComponentProperty d: componentProperties)
			{
				System.out.println(d);
				try
				{
					switch(d.type)
					{
						case COMPOUND:
						{
							JSONObject jsonDim = result.getJSONObject(d.name);
							break;
						}
						case ATTRIBUTE:
						{
//							JSONObject jsonDim = result.getJSONObject(d.name);
							String attribute = result.getString(d.name);
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
							model.addLiteral(observation,SDMXDIMENSION.refTime,model.createTypedLiteral(year+"-"+month+"-"+day, XSD.date.getURI()));							
						}						
					}
					
				}
				catch(Exception e)
				{
					cache.getCacheManager().shutdown();
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
		}			
	}

	/** Takes a json url of an openspending dataset model and extracts rdf into a jena model.  
	 * The DataStructureDefinition (DSD) specifies the structure of a dataset and contains a set of qb:ComponentSpecification resources.
	 * @param url json url that contains an openspending dataset model, e.g. http://openspending.org/fukuoka_2013/model  
	 * @param model initialized model that the triples will be added to
	 */
	static Resource createDataStructureDefinition(final URL url,Model model) throws MalformedURLException, IOException, JSONException
	{		
		Resource dsd = model.createResource(url.toString());
		model.add(dsd, RDF.type, QB.DataStructureDefinition);
		JSONObject dsdJson = readJSON(url);		
		JSONObject mapping = dsdJson.getJSONObject("mapping");
		for(Iterator<String> it = mapping.keys();it.hasNext();)
		{
			String key = it.next();
			JSONObject dimJson = mapping.getJSONObject(key);
			String type = dimJson.getString("type");
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
		}

		if(dsdJson.has("views"))
		{
			JSONArray views = dsdJson.getJSONArray("views");	
		}

		//		System.out.println("Converting dataset "+url);
		return dsd;
	}

	/** Takes the url of an openspending dataset and extracts rdf into a jena model.
	 * Each dataset contains a model which gets translated to a datastructure definition and entries that contain the actual measurements and get translated to a
	 * DataCube. 
	 * @param url json url that contains an openspending dataset, e.g. http://openspending.org/fukuoka_2013
	 * @param model initialized model that the triples will be added to
	 */
	static void createDataset(URL url,Model model) throws JSONException, IOException, LangDetectException
	{				
		System.out.println(url);
		JSONObject datasetJson = readJSON(new URL(url.toString()+".json"));
		String currency = null;
		if(datasetJson.has("currency"))
		{
			// TODO: do something with this
			currency = datasetJson.getString("currency");	
		}
		Resource dataSet = model.createResource(url.toString());
		Resource dsd = createDataStructureDefinition(new URL(url+"/model"), model);
		Set<ComponentProperty> componentProperties = createComponents(readJSON(new URL(url+"/model")).getJSONObject("mapping"), model,dataSet, dsd,currency);
				
		model.add(dataSet, RDF.type, QB.DataSet);
		model.add(dataSet, QB.structure, dsd);
		createEntries(new URL(url+"/entries.json"), model, dataSet,componentProperties);
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

	public static String readJSONString(URL url) throws MalformedURLException, IOException	
	{		
		System.out.println(cache.getKeys());
		Element e = cache.get(url.toString());
		if(e!=null) {/*System.out.println("cache hit for "+url.toString());*/return (String)e.getObjectValue();}
//		System.out.println("cache miss for "+url.toString());
		try(Scanner scanner = new Scanner(url.openStream(), "UTF-8"))
		{
			String datasetsJsonString = scanner.useDelimiter("\\A").next();			
			cache.put(new Element(url.toString(), datasetsJsonString));
			//IfAbsent
			return datasetsJsonString;
		}	
	}

	public static JSONObject readJSON(URL url) throws MalformedURLException, IOException, JSONException	
	{				
		return new JSONObject(readJSONString(url));		
	}

	public static JSONArray readJSONArray(URL url) throws MalformedURLException, IOException, JSONException	
	{		
		return new JSONArray(readJSONString(url));
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

	public static void main(String[] args) throws MalformedURLException, IOException, JSONException, LangDetectException
	{
		//		DetectorFactory.loadProfile("languageprofiles");
		Model model = ModelFactory.createMemModelMaker().createDefaultModel();
		model.setNsPrefix("qb", "http://purl.org/linked-data/cube#");
		model.setNsPrefix("os", OS);
		model.setNsPrefix("sdmx-subject",	"http://purl.org/linked-data/sdmx/2009/subject#");
		model.setNsPrefix("sdmx-dimension",	"http://purl.org/linked-data/sdmx/2009/dimension#");
		model.setNsPrefix("rdfs",RDFS.getURI());
		model.setNsPrefix("rdf",RDF.getURI());
		model.setNsPrefix("xsd",XSD.getURI());		
            
		//		JSONObject datasets = readJSON(new URL(DATASETS));
		//		JSONArray datasetArray =  datasets.getJSONArray("datasets");
		//		for(int i=0;i<datasetArray.length();i++)
		//		{
		//			JSONObject datasetJson = datasetArray.getJSONObject(i);
		//			URL url = new URL(datasetJson.getString("html_url"));
//		URL url = new URL("http://openspending.org/berlin_de");
		URL url = new URL("http://openspending.org/bmz-activities");
		
		createDataset(url,model);			
		//			if(i>0) break;
		//		}
		File folder = new File("output");
		folder.mkdir();		
		model.write(new PrintWriter(folder+"/"+url.toString().substring(url.toString().lastIndexOf('/')+1)+".ttl"),"TURTLE",null);
		CacheManager.getInstance().shutdown();
	}
}