package org.aksw.linkedspending.downloader;

import lombok.extern.java.Log;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Class to create a thread. The threads purpose is to monitor the threadpool, which does the
 * downloading of all JSON-files from openspending.
 */
@Log class ThreadMonitor extends Thread
{
	// final Collection<DownloadCallable> callables;
	// public ThreadMonitor(Collection<DownloadCallable> callables) {this.callables=callables;}
	final ThreadPoolExecutor	executor;

	public ThreadMonitor(ThreadPoolExecutor executor)
	{
		this.executor = executor;
	}

	boolean	running	= true;

	public void stopMonitoring()
	{
		running = false;
	}

	@Override public void run()
	{
		while (running)
		{
			try
			{
				Thread.sleep(5000);
			}
			catch (InterruptedException e)
			{
				log.warning("interrupted thread monitor");
			}
			// synchronized(callables)
			// {
			// for(DownloadCallable callable: callables)
			// {
			// nrs.add(callable.nr);
			// }
			// System.out.println("Active threads:"+nrs);
			// }
			log.finer(executor.getCompletedTaskCount() + " completed, " + executor.getActiveCount() + " active.");
		}
		log.fine("stopped monitoring");
	}
}
