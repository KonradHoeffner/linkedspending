package org.aksw.linkedspending.job;

import lombok.AllArgsConstructor;
import org.aksw.linkedspending.convert.ConvertWorker;
import org.aksw.linkedspending.download.DownloadWorker;

@AllArgsConstructor
public class Processor
{
	final String datasetName;
	final Job job;

	public void process()
	{
		DownloadWorker downloader = new DownloadWorker(datasetName,job,false);
		downloader.get();
		downloader=null;
		ConvertWorker converter = new ConvertWorker(datasetName,job,false);
//		converter.c
		converter=null;

	}
}
