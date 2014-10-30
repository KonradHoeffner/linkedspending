package org.aksw.linkedspending.job;

import java.util.function.Supplier;

/** Workers are responsible to set the job states to STOPPED, PAUSED and RUNNING, respectively, when the requested operation takes effect.
 * The fail state is managed by the Job class when the call method throws an exception.
 * The return of call states is true when the worker successfully completed, false if it has been stopped.*/
public abstract class Worker implements Supplier<Boolean>
{
	protected volatile boolean stopRequested = false;
	protected volatile boolean pauseRequested = false;

	public void stop()	{stopRequested=true;}
	public void pause() {pauseRequested=true;}
	public void resume() {pauseRequested=false;notify();}

	protected final Job job;
	protected final String	datasetName;
	protected final boolean force;

	/**@param datasetName name of the dataset, e.g. "2013"
	 * @param job the job will get notified of state changes and progress estimates
	 * @param force force execution even if it is already done (e.g. file already converted and modification date after download date).
	 * use it if a bug has been fixed or the modelling has changed.
	 */
	public Worker(String datasetName, Job job, boolean force)
	{
		this.datasetName = datasetName;
		this.job = job;
		this.force = force;
	}

	/**insert this into long running loops together with "if(stopRequested) {break;}"
	 * @throws InterruptedException */
	protected void pausePoint(Object instance) throws InterruptedException
	{
		synchronized(instance)
		{
			while(pauseRequested&&!stopRequested)
			{
				job.setState(State.PAUSED);
				wait();
			}
			if(!stopRequested&&(job.getState()==State.PAUSED)) {job.setState(State.RUNNING);}
		}
	}
}