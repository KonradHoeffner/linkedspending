package org.aksw.linkedspending.job;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.java.Log;
import org.aksw.linkedspending.LinkedSpendingDatasetInfo;
import org.aksw.linkedspending.OpenSpendingDatasetInfo;

/** periodically gets called, starts one job if possible and then disappears again*/
@Log
public class Boss implements Runnable
{
	private static final long	TIMEOUT_HOURS	= 1;
	static private boolean FORCE = false;
	static private boolean PREFER_UNCONVERTED_TO_OUTDATED = false;

	static private final boolean RANDOM = true;
	static final Random random = RANDOM?new Random():null;

	static AtomicInteger threadCount = new AtomicInteger(0);
	int nr = threadCount.incrementAndGet();


	@Override public void run()
	{
		if(!OpenSpendingDatasetInfo.isOnline())
		{
			try
			{
				log.warning("OpenSpending is offline. Boss thread "+nr+" waiting.");
				OpenSpendingDatasetInfo.onlineLock.lock();
				OpenSpendingDatasetInfo.onlineCondition.await();
			}
			catch (InterruptedException e)
			{
				log.info("Boss thread "+nr+" interrupted.");
				return;
			}
			finally
			{
				OpenSpendingDatasetInfo.onlineLock.unlock();
			}
			log.info("OpenSpending is online again. Boss thread "+nr+" continuing.");
		}
		String datasetName = null;
		Job job = null;
		try
		{
			synchronized(Job.class)
			{
				// must not throw any exception because ScheduledExecutorService.scheduleAtFixedRate does not schedule any more after exceptions
				log.info("Boss started (random order mode "+RANDOM+")");
				Map<String, LinkedSpendingDatasetInfo> lsInfos = LinkedSpendingDatasetInfo.cached();
				Map<String, OpenSpendingDatasetInfo> osInfos = OpenSpendingDatasetInfo.getDatasetInfosCached();
				// first priority: unconverted datasets

				Set<String> unconverted = osInfos.keySet();
				unconverted.removeAll(lsInfos.keySet());
				// don't choose one that is already being worked on or was worked on in the past (stopped or failed)
				unconverted.removeAll(Job.all());
				Set<String> pool = new HashSet<>(unconverted);
				if(unconverted.isEmpty()||!PREFER_UNCONVERTED_TO_OUTDATED)
				{
					Set<String> needRefresh = osInfos.keySet().stream().filter(s->LinkedSpendingDatasetInfo.upToDate(s)&&LinkedSpendingDatasetInfo.newestTransformation(s))
							.collect(Collectors.toSet());
					pool.addAll(needRefresh);

				}
				if(!pool.isEmpty())
				{
					datasetName = RANDOM?pool.toArray(new String[0])[random.nextInt(pool.size())]:pool.iterator().next();
					log.info("Boss starting "+(unconverted.contains(datasetName)?"unconverted":"outdated")+" dataset "+datasetName);
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
				job.worker=null;
				job.setState(State.FAILED);
				String timeoutMessage = "Timeout limit of "+TIMEOUT_HOURS+" hours exceeded for dataset "+datasetName;
				log.severe(timeoutMessage+", progress: "+job.json().toString());
				job.addHistory(timeoutMessage);
			}
		}
		catch(Exception e)
		{
			if(job!=null)
			{
				job.setState(State.FAILED);
				job.addHistory(e.getClass()+": "+e.getMessage()+": "+Arrays.toString(e.getStackTrace()));
			}
			log.severe("Dataset "+datasetName+": Unexpected exception: "+e.getMessage());
			e.printStackTrace();
		}
	}
}