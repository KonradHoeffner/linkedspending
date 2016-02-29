package org.aksw.linkedspending.job;

import static org.aksw.linkedspending.job.State.RUNNING;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Optional;
import org.aksw.linkedspending.convert.ConvertWorker;
import org.aksw.linkedspending.download.DownloadWorker;
import org.aksw.linkedspending.upload.UploadWorker;

public class DownloadConvertUploadWorker extends WorkerSequence
{
	public DownloadConvertUploadWorker(String datasetName, Job job, boolean force)
	{
		this(datasetName,job,force,force,force);
	}

	public DownloadConvertUploadWorker(String datasetName, Job job, boolean forceDownload, boolean forceConvert, boolean forceUpload)
	{
		super(datasetName, job,
				new LinkedList<WorkerGenerator>	(Arrays.asList(DownloadWorker::new,ConvertWorker::new,UploadWorker::new)),
				new LinkedList<>			(Arrays.asList(forceDownload,forceConvert,forceUpload)));
	}

	@Override public Boolean get()
	{
		job.setState(RUNNING);
		boolean success = super.get();
		if(success) {job.setState(State.FINISHED);}
		job.worker=Optional.empty();
		return success;
	}

}