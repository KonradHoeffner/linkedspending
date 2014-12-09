package org.aksw.linkedspending.job;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.java.Log;
import org.aksw.linkedspending.LinkedSpendingDatasetInfo;
import org.aksw.linkedspending.OpenSpendingDatasetInfo;

/** periodically gets called, starts one job if possible and then disappears again*/
@Log
public class Boss implements Runnable
{
	static private boolean FORCE = false;

	@Override public void run()
	{
		String datasetName = null;
		Job job = null;
		try
		{
			synchronized(Job.class)
			{
				// must not throw any exception because ScheduledExecutorService.scheduleAtFixedRate does not schedule any more after exceptions
				log.info("Boss started");
				Map<String, LinkedSpendingDatasetInfo> lsInfos = LinkedSpendingDatasetInfo.all();
				Map<String, OpenSpendingDatasetInfo> osInfos = OpenSpendingDatasetInfo.getDatasetInfosCached();
				// first priority: unconverted datasets
				Set<String> unconverted = osInfos.keySet();
				unconverted.removeAll(lsInfos.keySet());
				// don't choose one that is already being worked on or was worked on in the past (stopped or failed)
				unconverted.removeAll(Job.all());

				if(!unconverted.isEmpty())
				{
					datasetName = unconverted.iterator().next();
					log.info("Boss starting unconverted dataset "+datasetName);
				} else // are there already converted but outdated ones or ones with an old transformation?
				{
					Set<String> needRefresh = osInfos.keySet().stream().filter(s->LinkedSpendingDatasetInfo.upToDate(s)&&LinkedSpendingDatasetInfo.newestTransformation(s))
							.collect(Collectors.toSet());

					if(!needRefresh.isEmpty())
					{
						datasetName = needRefresh.iterator().next();
						log.info("Boss starting outdated dataset "+datasetName);
					}
				}
				if(datasetName!=null)
				{
					job = Job.forDatasetOrCreate(datasetName);
				}
			}
			if(datasetName!=null)
			{
				boolean finished = new DownloadConvertUploadWorker(datasetName, job, FORCE).get();
				if(finished)
				{
					// TODO check for memory leaks (references)
					Job.jobs.remove(job);
				}
			}
		}
		catch(Exception e)
		{
			if(job!=null)
			{
				job.setState(State.FAILED);
				job.addHistory(e.getMessage()+": "+Arrays.toString(e.getStackTrace()));
			}
			log.severe("Dataset "+datasetName+": Unexpected exception: "+e.getMessage());
			e.printStackTrace();
		}
	}

}