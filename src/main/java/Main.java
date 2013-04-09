import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Scanner;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class Main
{
	static final String DATASETS = "http://openspending.org/datasets.json";	

	static public class QB
	{
		static final String qb = "http://purl.org/linked-data/cube#";
		static final Resource DataStructureDefinition = ResourceFactory.createResource(qb+"DataStructureDefinition");
		static final Resource DataSet = ResourceFactory.createResource(qb+"DataSet");
	}

	static void createDataStructureDefinition(URL url,Model model)
	{		
		Resource dsd = model.createResource(url.toString());
		model.add(dsd, RDF.type, QB.DataStructureDefinition);
		//		System.out.println("Converting dataset "+url);
	}


	static void createDataset(URL url,Model model) throws MalformedURLException
	{
		createDataStructureDefinition(new URL(url.toString()+"/model"), model);		
		Resource dataSet = model.createResource(url.toString());
		model.add(dataSet, RDF.type, QB.DataSet);
		model.add(dataSet, RDFS.label, QB.DataSet);
		//		model.createStatement(arg0, arg1, arg2)
		//		System.out.println("Converting dataset "+url);

	}

	public static JSONObject readJSON(URL url) throws MalformedURLException, IOException, JSONException
	{
		try(Scanner scanner = new Scanner(new URL(DATASETS).openStream(), "UTF-8"))
		{
			String datasetsJsonString = scanner.useDelimiter("\\A").next();
			return new JSONObject(datasetsJsonString);
		}		
	}

	public static void main(String[] args) throws MalformedURLException, IOException, JSONException
	{
		Model model = ModelFactory.createMemModelMaker().createDefaultModel();
		model.setNsPrefix("qb", "http://purl.org/linked-data/cube#");
		model.setNsPrefix("os", "http://openspending.org/");
		model.setNsPrefix("sdmx-subject",	"http://purl.org/linked-data/sdmx/2009/subject#");

		JSONObject datasets = readJSON(new URL(DATASETS));
		JSONArray datasetArray =  datasets.getJSONArray("datasets");
		for(int i=0;i<datasetArray.length();i++)
		{
			JSONObject datasetJson = datasetArray.getJSONObject(i);
			URL url = new URL(datasetJson.getString("html_url"));
			createDataset(url,model);
		}
		model.write(new PrintWriter("test.ttl"),"TURTLE",null);
	}
}