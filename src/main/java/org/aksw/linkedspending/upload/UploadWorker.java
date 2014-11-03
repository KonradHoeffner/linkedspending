package org.aksw.linkedspending.upload;

import java.io.File;
import lombok.extern.java.Log;
import org.aksw.linkedspending.DataSetFiles;
import org.aksw.linkedspending.job.Job;
import org.aksw.linkedspending.job.Phase;
import org.aksw.linkedspending.job.Worker;
import org.aksw.linkedspending.tools.PropertiesLoader;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoUpdateFactory;
import virtuoso.jena.driver.VirtuosoUpdateRequest;

@Log
public class UploadWorker extends Worker
{

	// using virt-jena, doesn't work
	static void uploadDataSet(String datasetName)
	{
		VirtGraph graph = new VirtGraph("jdbc:virtuoso://[2001:638:902:2010:0:168:35:119]:1111",
				PropertiesLoader.virtuosoUser,PropertiesLoader.virtuosoPassword);
		VirtuosoUpdateRequest request = VirtuosoUpdateFactory.read(new File(DataSetFiles.RDF_FOLDER,datasetName+".nt").getAbsolutePath(),graph);
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
		log.severe("Instantiating Dummy Upload Worker.");
	}

	@Override public Boolean get()
	{
		log.severe("Upload Worker: dummy get for tests");
		job.setPhase(Phase.UPLOAD);
		return true;
	}

}
