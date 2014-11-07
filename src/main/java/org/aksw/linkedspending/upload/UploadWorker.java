package org.aksw.linkedspending.upload;

import static org.aksw.linkedspending.tools.PropertyLoader.*;
import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import lombok.extern.java.Log;
import org.aksw.linkedspending.DataSetFiles;
import org.aksw.linkedspending.Virtuoso;
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
			GraphUtil.add(virtGraph, it);
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
		uploadDataSet(datasetName);
		job.setPhase(Phase.UPLOAD);
		return true;
	}

}