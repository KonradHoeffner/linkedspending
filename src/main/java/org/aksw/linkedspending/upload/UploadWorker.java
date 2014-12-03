package org.aksw.linkedspending.upload;

import static org.aksw.linkedspending.tools.PropertyLoader.*;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import lombok.extern.java.Log;
import org.aksw.linkedspending.DataSetFiles;
import org.aksw.linkedspending.Virtuoso;
import org.aksw.linkedspending.job.Job;
import org.aksw.linkedspending.job.Phase;
import org.aksw.linkedspending.job.Worker;
import org.aksw.linkedspending.tools.DataModel;
import org.aksw.linkedspending.tools.PropertyLoader;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RiotReader;
import virtuoso.jena.driver.VirtGraph;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.GraphUtil;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

@Log
public class UploadWorker extends Worker
{

	static void uploadDataSet(String datasetName)
	{
		// delete subgraph where the old dataset resides
		Virtuoso.deleteSubGraph(datasetName);
		Virtuoso.createSubGraph(datasetName);
		//		Model model = FileManager.get().loadModel(new File(DataSetFiles.RDF_FOLDER,datasetName+".nt").getAbsolutePath());
		try(FileInputStream in = new FileInputStream(new File(DataSetFiles.RDF_FOLDER,datasetName+".nt").getAbsolutePath()))
		{
			VirtGraph virtGraph = new VirtGraph(graph+datasetName,jdbcUrl,jdbcUser,jdbcPassword);
			Iterator<Triple> it = RiotReader.createIteratorTriples(in, Lang.NT, "");
			Instant start = Instant.now();
			virtGraph.getBulkUpdateHandler().add(it);
//			log.info(Duration.between(start,Instant.now()));

			// mark complete upload so that partial uploads can be detected by this missing
			virtGraph.add(Triple.create(
					Node.createURI(PropertyLoader.prefixInstance+datasetName),
					DataModel.LSOntology.uploadComplete.asNode(),
					Node.createLiteral("true",XSDDatatype.XSDboolean)));
			virtGraph.close();
		}
		catch(Exception e)
		{
			throw new RuntimeException("could not upload dataset '"+datasetName+"'"+e);
		}
	}

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
		job.setPhase(Phase.UPLOAD);
		log.info("Starting upload of "+datasetName);
		uploadDataSet(datasetName);
		job.uploadProgressPercent.set(100);
		log.info("Finished upload of "+datasetName);
		return true;
	}

}