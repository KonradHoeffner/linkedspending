package org.aksw.linkedspending.job;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

public class WorkerSequence extends Worker
{
	final Set<Worker> workers = new HashSet<>();

	final Queue<WorkerGenerator> workerGenerators;
	final Queue<Boolean> forces;

	public WorkerSequence(String datasetName, Job job, Queue<WorkerGenerator> workerGenerators, Queue<Boolean> forces)
	{
		// TODO: what to do with super field force?
		super(datasetName, job, forces.stream().max(Boolean::compare).get());
		this.forces=forces;
		if(workerGenerators.isEmpty()) {throw new IllegalArgumentException("no worker generators provided");}
		this.workerGenerators = workerGenerators;
	}

	@Override public Boolean get()
	{
		while(!workerGenerators.isEmpty())
		{
			if(stopRequested) {job.setState(State.STOPPED); return false;}
			Worker w = workerGenerators.poll().apply(datasetName, job, forces.poll());
			workers.add(w);
			if(!w.get()) {return false;}
			workers.clear();
		}
		return true;
	}

	@Override public void stop()   {for(Worker w: workers) {w.stop();}}
	@Override public void pause()  {for(Worker w: workers) {w.pause();}}
	@Override public void resume() {for(Worker w: workers) {w.resume();}}
}