package org.aksw.linkedspending.job;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.extern.java.Log;
import org.aksw.linkedspending.LinkedSpendingDatasetInfo;
import org.aksw.linkedspending.OpenSpendingDatasetInfo;

/** periodically gets called, starts one job if possible and then disappears again*/
@Log
public class Boss implements Runnable
{
	private static final long	TIMEOUT_HOURS	= 4;
	static private boolean FORCE = false;

	static private final boolean RANDOM = true;
	static final Random random = RANDOM?new Random():null;

	@Override public void run()
	{
		String datasetName = null;
		Job job = null;
		try
		{
			synchronized(Job.class)
			{
				// must not throw any exception because ScheduledExecutorService.scheduleAtFixedRate does not schedule any more after exceptions
				log.info("Boss started (random order mode "+RANDOM+")");
				Map<String, LinkedSpendingDatasetInfo> lsInfos = LinkedSpendingDatasetInfo.all();
				Map<String, OpenSpendingDatasetInfo> osInfos = OpenSpendingDatasetInfo.getDatasetInfosCached();
				// first priority: unconverted datasets

				Set<String> unconverted = osInfos.keySet();
				unconverted.removeAll(lsInfos.keySet());
				// don't choose one that is already being worked on or was worked on in the past (stopped or failed)
				unconverted.removeAll(Job.all());

				if(!unconverted.isEmpty())
				{
					datasetName = RANDOM?unconverted.toArray(new String[0])[random.nextInt(unconverted.size())]:unconverted.iterator().next();
					log.info("Boss starting unconverted dataset "+datasetName);
				} else // are there already converted but outdated ones or ones with an old transformation?
				{
					Set<String> needRefresh = osInfos.keySet().stream().filter(s->LinkedSpendingDatasetInfo.upToDate(s)&&LinkedSpendingDatasetInfo.newestTransformation(s))
							.collect(Collectors.toSet());

					if(!needRefresh.isEmpty())
					{
						datasetName = RANDOM?needRefresh.toArray(new String[0])[random.nextInt(unconverted.size())]:needRefresh.iterator().next();
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
//				Executors.newSingleThreadExecutor().;
				CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(new DownloadConvertUploadWorker(datasetName, job, FORCE));
				boolean finished = future.get(TIMEOUT_HOURS, TimeUnit.HOURS);

//				boolean finished = new DownloadConvertUploadWorker(datasetName, job, FORCE).get();
				if(finished)
				{
					// TODO check for memory leaks (references)
					Job.jobs.remove(job);
				}
			}
		}
		catch(TimeoutException e)
		{
			if(job!=null)
			{
				job.setState(State.FAILED);
				String timeoutMessage = "Timeout limit of "+TIMEOUT_HOURS+" hours exceeded for dataset "+datasetName;
				log.severe(timeoutMessage+", progress: "+job.json().toString());
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