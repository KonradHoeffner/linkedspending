package org.aksw.linkedspending.upload;

import static org.aksw.linkedspending.tools.PropertiesLoader.*;
import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import lombok.extern.java.Log;
import org.aksw.linkedspending.DataSetFiles;
import org.aksw.linkedspending.job.Job;
import org.aksw.linkedspending.job.Phase;
import org.aksw.linkedspending.job.Worker;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RiotReader;
import virtuoso.jena.driver.VirtGraph;
import com.hp.hpl.jena.graph.GraphUtil;
import com.hp.hpl.jena.graph.Triple;

@Log
public class UploadWorker extends Worker
{

	// using virt-jena, doesn't work
	static void uploadDataSet(String datasetName)
	{
//		Model model = FileManager.get().loadModel(new File(DataSetFiles.RDF_FOLDER,datasetName+".nt").getAbsolutePath());
		try(FileInputStream in = new FileInputStream(new File(DataSetFiles.RDF_FOLDER,datasetName+".nt").getAbsolutePath()))
		{
			VirtGraph graph = new VirtGraph(virtuosoGraph,virtuosoUrl,virtuosoUser,virtuosoPassword);
			Iterator<Triple> it = RiotReader.createIteratorTriples(in, Lang.NT, "");
			GraphUtil.add(graph, it);
			graph.close();
		}
		catch(Exception e)
		{
			throw new RuntimeException("could not upload dataset '"+datasetName+"'"+e);
		}

//		GraphUtil.addInto(graph, model.getGraph());

		//		graph.getBulkUpdateHandler().

		//		VirtuosoUpdateRequest request = VirtuosoUpdateFactory.read(new File(DataSetFiles.RDF_FOLDER,datasetName+".nt").getAbsolutePath(),graph);
		//		System.out.println(request);
		//		request.exec();
	}

	//using normal jena
	//	static void uploadDataSet(String datasetName)
	//	{
	//		HttpContext httpContext = new BasicHttpContext();
	//
	////		UpdateRequest request = UpdateFactory.read(new File(DataSetFiles.RDF_FOLDER,datasetName+".nt").getAbsolutePath());
	//		String query = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>"
	//				+ "WITH <http://example/addresses> INSERT { <http://test.de/William> foaf:givenName 'William' } WHERE { <http://test.de/William> foaf:givenName 'William' }";
	//		System.out.println(query);
	//		UpdateRequest request = UpdateFactory.create(query);
	//		UpdateProcessor processor = UpdateExecutionFactory
	//			    .createRemoteForm(request, "http://linkedspending.aksw.org/sparql");
	//			((UpdateProcessRemoteForm)processor).setHttpContext(httpContext);
	//			processor.execute();
	//	}

	public UploadWorker(String datasetName, Job job, boolean force)
	{
		super(datasetName, job, force);
	}

	@Override public Boolean get()
	{
		if(!force)
		{
			// TODO identify whether dataset of same or later creation data already exists
		}
		uploadDataSet(datasetName);
		job.setPhase(Phase.UPLOAD);
		return true;
	}

}