package org.aksw.linkedspending.job;

import lombok.AllArgsConstructor;
import org.aksw.linkedspending.converter.Converter;
import org.aksw.linkedspending.downloader.DownloadCallable;

@AllArgsConstructor
public class Processor
{
	final String datasetName;
	final Job job;

	public void process()
	{
		DownloadCallable downloader = new DownloadCallable(datasetName,job);
		downloader.call();
		downloader=null;
		Converter converter = new Converter();
//		converter.c
		converter=null;

	}
}
