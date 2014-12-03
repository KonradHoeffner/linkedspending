package org.aksw.linkedspending.job;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

public class WorkerSequence extends Worker
{
	final Set<Worker> workers = new HashSet<>();

	final Queue<WorkerGenerator> workerGenerators;

	public WorkerSequence(String datasetName, Job job, boolean force, Queue<WorkerGenerator> workerGenerators)
	{
		super(datasetName, job, true);
		if(workerGenerators.isEmpty()) {throw new IllegalArgumentException("no worker generators provided");}
		this.workerGenerators = workerGenerators;
	}

	@Override public Boolean get()
	{
		while(!workerGenerators.isEmpty())
		{
			if(stopRequested) {job.setState(State.STOPPED); return false;}
			Worker w = workerGenerators.poll().apply(datasetName, job, force);
			workers.add(w);
			if(!w.get()) {return false;}
			workers.clear();
		}
		return true;
	}

	public void stop()   {for(Worker w: workers) {w.stop();}}
	public void pause()  {for(Worker w: workers) {w.pause();}}
	public void resume() {for(Worker w: workers) {w.resume();}}
}