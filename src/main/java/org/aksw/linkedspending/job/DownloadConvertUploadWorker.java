package org.aksw.linkedspending.job;

import static org.aksw.linkedspending.job.State.RUNNING;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Optional;
import org.aksw.linkedspending.LinkedSpendingDatasetInfo;
import org.aksw.linkedspending.convert.ConvertWorker;
import org.aksw.linkedspending.download.DownloadWorker;
import org.aksw.linkedspending.upload.UploadWorker;

public class DownloadConvertUploadWorker extends WorkerSequence
{
	public DownloadConvertUploadWorker(String datasetName, Job job, boolean force)
	{
		super(datasetName, job, force, new LinkedList<WorkerGenerator>
		(Arrays.asList(DownloadWorker::new,ConvertWorker::new,UploadWorker::new)));
	}

	@Override public Boolean get()
	{
		job.setState(RUNNING);
		boolean success;
		// don't do anything if already on SPARQL endpoint and no new data available
		if(!force&&LinkedSpendingDatasetInfo.upToDateAndNewestTransformation(datasetName))
		{
			success=true;
			job.addHistory("Dataset already on endpoint and up to date, force is not set -> skipped.");
		}
		else
		{
			success = super.get();
		}
		if(success) {job.setState(State.FINISHED);}
		job.worker=Optional.empty();
		return success;
	}

}