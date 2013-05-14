import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.cybozu.labs.langdetect.Language;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

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
		static final Resource DimensionProperty = ResourceFactory.createResource(qb+"DimensionProperty");
		static final Property dimension = ResourceFactory.createProperty(qb+"dimension");
	}
	
		/** @param url dimensions url, e.g. http://openspending.org/berlin_de/dimensions.json	 */

	static void createDimensions(URL url, Model model, Resource dsd) throws MalformedURLException, IOException, JSONException
	{
		JSONArray dimensions = readJSONArray(url);		
			for(int i=0;i<dimensions.length();i++)
			{
				JSONObject dimJson = dimensions.getJSONObject(i);
				Resource dim = model.createResource(dimJson.getString("html_url"));				
				model.add(dsd, QB.dimension, dim);
				model.add(dim, RDF.type, QB.dimension);
				
				String label = dimJson.getString("label");
				if(label!=null&&!label.equals("null")) {model.add(dim,RDFS.label,label);}
				String description = dimJson.getString("description");
				if(description!=null&&!description.equals("null")) {model.add(dim,RDFS.comment,description);}				
	
				String type = dimJson.getString("type");
//				switch(type)
//				{
//					case "compound":return;
//					case "measure":return;
//					case "date":return;
//					case "attribute":return;
//				}
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
		Resource dsd = createDataStructureDefinition(new URL(url.toString()+"/model"), model);
		createDimensions(new URL(url+"/dimensions.json"), model, dsd);
		Resource dataSet = model.createResource(url.toString());		
		model.add(dataSet, RDF.type, QB.DataSet);
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
		Element e = cache.get(url.toString());
		if(e!=null) {return (String)e.getObjectValue();}
		System.out.println("cache miss");
		try(Scanner scanner = new Scanner(url.openStream(), "UTF-8"))
		{
			String datasetsJsonString = scanner.useDelimiter("\\A").next();			
			cache.putIfAbsent(new Element(url.toString(), datasetsJsonString));
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
		model.setNsPrefix("rdfs",RDFS.getURI());
		model.setNsPrefix("rdf",RDF.getURI());

		JSONObject datasets = readJSON(new URL(DATASETS));
		JSONArray datasetArray =  datasets.getJSONArray("datasets");
		for(int i=0;i<datasetArray.length();i++)
		{
			JSONObject datasetJson = datasetArray.getJSONObject(i);
			URL url = new URL(datasetJson.getString("html_url"));
				
			createDataset(url,model);
			if(i>3) break;
		}
		model.write(new PrintWriter("test.ttl"),"TURTLE",null);
		CacheManager.getInstance().shutdown();
	}
}