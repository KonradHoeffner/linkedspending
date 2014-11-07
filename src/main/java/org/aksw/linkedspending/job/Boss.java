package org.aksw.linkedspending.job;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.aksw.linkedspending.LinkedSpendingDatasetInfo;
import org.aksw.linkedspending.OpenSpendingDatasetInfo;

/** periodically gets called, starts one job if possible and then disappears again*/
public class Boss implements Runnable
{

	@Override public void run()
	{
		Map<String, LinkedSpendingDatasetInfo> lsInfos = LinkedSpendingDatasetInfo.all();
		Map<String, OpenSpendingDatasetInfo> osInfos = OpenSpendingDatasetInfo.getDatasetInfosFresh();
		// first priority: unconverted datasets
		Set<String> unconverted = osInfos.keySet();
		unconverted.removeAll(lsInfos.keySet());

		String datasetName = null;

		if(!unconverted.isEmpty())
		{
			datasetName = unconverted.iterator().next();
		} else // are there already converted but outdated ones?
		{
			Set<String> converted = osInfos.keySet();
			converted.removeAll(unconverted);
			Set<String> outdated = converted.stream().filter(s->lsInfos.get(s).modified.isBefore(osInfos.get(s).modified)).collect(Collectors.toSet());

			// TODO use the most outdated one
			if(!outdated.isEmpty())
			{
				datasetName = outdated.iterator().next();
			}
		}
		if(datasetName!=null)
		{
			Job job = null;
			try
			{
				job = Job.forDataset(datasetName);
				boolean finished = new DownloadConvertUploadWorker(datasetName, job, true).get();
				if(finished)
				{
					// TODO check for memory leaks (references)
					Job.jobs.remove(job);
				}

			}
			catch (Exception e)
			{
				if(job!=null)
				{
					// TODO allow restarting of failed state
					job.setState(State.FAILED);
					job.addHistory(e.getMessage());
				}
			}
		}

		// TODO detect partially uploaded ones, save number of entries?
		// TODO prevent endless repeats of failed downloads
		// TODO remove finished jobs
	}

}