package org.aksw.linkedspending.job;

import static org.aksw.linkedspending.job.Operation.*;
import static org.aksw.linkedspending.job.Phase.*;
import static org.aksw.linkedspending.job.State.*;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import lombok.extern.java.Log;
import org.aksw.linkedspending.OpenSpendingDatasetInfo;
import org.aksw.linkedspending.exception.DataSetDoesNotExistException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Represents the state of a dataset job, which can be conversion, download or upload. */
@Log
@Singleton
@Path("/jobs")
public class Job
{
	public AtomicInteger downloadProgressPercent = new AtomicInteger(0);
	public AtomicInteger convertProgressPercent = new AtomicInteger(0);
	public AtomicInteger uploadProgressPercent = new AtomicInteger(0);

	static final String ROOT_PREFIX = "http://localhost:10010/";
	static final String PREFIX = ROOT_PREFIX+"jobs/";

	final public String url;
	final String datasetName;

	private State state = CREATED;
	private Phase phase = DOWNLOAD;

	DownloadConvertUploadWorker worker = null;

	SortedMap<Long,Object> history = new TreeMap<>();

	public void addHistory(Object o)
	{
		synchronized(history)
		{
			long now = Instant.now().toEpochMilli();
			// We don't value millisecond accuracy that much and messages are not expected to occur so frequently
			// but dropped messages are really bad.
			while(history.containsKey(now)) now++;
			history.put(now, o);
		}
	}

	/** dont use it, its for jersey */
	public Job() {datasetName="dummyForJersey";this.phase=DOWNLOAD;this.url=uriOf(datasetName);}

	static public Map<String,Job> jobs = new HashMap<>();

	@GET @Path("") @Produces("application/json")
	public static String jobs() throws IOException
	{
		//		jobs.add(new Job(""+new Random().nextInt()));
		ObjectMapper mapper = new ObjectMapper();
		ArrayNode rootNode = mapper.createArrayNode();

		for(Job job:Job.jobs.values()) {rootNode.add(job.json());}

		return rootNode.toString();
	}

	@GET @Path("/{datasetname}") @Produces(MediaType.APPLICATION_JSON)
	public static String json(@PathParam("datasetname") String datasetName) //throws DataSetDoesNotExistException
	{
		try{return Job.forDataset(datasetName).json().toString();}
		catch(DataSetDoesNotExistException e) {return e.getMessage();}
	}

	@GET @Path("/{datasetname}/{operation}") @Produces(MediaType.TEXT_PLAIN)
	public static String operation(@PathParam("datasetname") String datasetName,@PathParam("operation") String operationName) throws InterruptedException
	{
		try
		{
			Job job = Job.forDataset(datasetName);
			try
			{
				Operation op = Operation.valueOf(operationName.toUpperCase());
				if(!job.getOperations().contains(op)) {return "operation \""+operationName+"\" not possible in state \""+job.state+"\". Nothing done.";}
				switch(op)
				{
					case START:return String.valueOf(job.start());
					case STOP:job.stop();break;
					default:;
				}
				return "todo: operation "+op+" on dataset "+datasetName;
			}
			catch(IllegalArgumentException e)
			{
				return "operation \""+operationName+"\" does not exist. Nothing done.";
			}
		}
		catch(DataSetDoesNotExistException e) {return e.getMessage();}
	}

	private void stop()
	{

	}

	private Job(String datasetName)
	{
		//		future.ex
		this.datasetName = datasetName;
		this.url=uriOf(datasetName);
		//		this.prefix=url+'/';
		addHistory(CREATED);
		addHistory(DOWNLOAD);
	}

	synchronized public static Job forDataset(String datasetName) throws DataSetDoesNotExistException
	{
		if(!OpenSpendingDatasetInfo.getDatasetInfosCached().keySet().contains(datasetName)) throw new DataSetDoesNotExistException(datasetName);
		synchronized(jobs)
		{
			Job job = jobs.get(datasetName);
			if(job==null)
			{
				job=new Job(datasetName);
				jobs.put(datasetName, job);
			}
			return job;
		}
	}

	static final Map<Phase,EnumSet<Phase>> phaseTransitions;
	static
	{
		Map<Phase,EnumSet<Phase>> t = new HashMap<>();

		t.put(DOWNLOAD,EnumSet.of(CONVERT));
		t.put(CONVERT,EnumSet.of(UPLOAD));
		t.put(UPLOAD,EnumSet.noneOf(Phase.class));
		phaseTransitions = Collections.unmodifiableMap(t);
	}

	/** @param newPhase the new phase the job shall go into
	 * @return true iff the job is running and the phase transition is possible
	 */
	public boolean setPhase(Phase newPhase)
	{
		synchronized(state)
		{
			synchronized(phase)
			{
				// if stopped, paused, finished or crashed job doesn't move forward so phase can't change
				if(state!=RUNNING) {return false;}
				if(phaseTransitions.get(phase).contains(newPhase))
				{
					addHistory(newPhase);
					phase = newPhase;
					return true;
				}
				return false;
			}
		}
	}

	static final Map<State,EnumSet<State>> transitions;
	static
	{
		Map<State,EnumSet<State>> t = new HashMap<>();

		t.put(CREATED,EnumSet.of(RUNNING,STOPPED));
		t.put(RUNNING,EnumSet.of(PAUSED,FINISHED,FAILED,STOPPED));
		t.put(PAUSED,EnumSet.of(RUNNING,STOPPED));
		t.put(FINISHED, EnumSet.noneOf(State.class));
		t.put(FAILED, EnumSet.noneOf(State.class));
		t.put(STOPPED, EnumSet.noneOf(State.class));
		transitions = Collections.unmodifiableMap(t);
	}

	static SortedMap<State,EnumSet<Operation>> operations = new TreeMap<>();
	static
	{
		SortedMap<State,EnumSet<Operation>> s = new TreeMap<>();
		s.put(CREATED, EnumSet.of(START,STOP));
		s.put(RUNNING, EnumSet.of(STOP,PAUSE));
		s.put(PAUSED, EnumSet.of(STOP,RESUME));

		s.put(STOPPED, EnumSet.noneOf(Operation.class));
		s.put(FINISHED, EnumSet.noneOf(Operation.class));
		s.put(PAUSED, EnumSet.noneOf(Operation.class));

		operations = Collections.unmodifiableSortedMap(s);
	}

	public EnumSet<Operation> getOperations() {return operations.get(state);}

	public Phase getPhase() {return phase;}
	public State getState() {return state;}

	final long created = Instant.now().toEpochMilli();

	public static String uriOf(String datasetName) {return PREFIX+datasetName;}

	public boolean setState(State newState)
	{
		synchronized(state)
		{
			if(transitions.get(state).contains(newState))
			{
				addHistory(newState);
				state=newState;
				return true;
			}
			return false;
		}
	}

	public synchronized boolean start() throws InterruptedException
	{
		if(setState(RUNNING))
		{
			try
			{
				DownloadConvertUploadWorker worker = new DownloadConvertUploadWorker (datasetName,this,false);
				CompletableFuture.supplyAsync(worker);
			}
			catch (Exception e)
			{
				log.severe(e.getMessage());
				addHistory(e.getMessage());
				setState(FAILED);
			}

			return true;
		}
		return false;
	}

	public ObjectNode json()
	{
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode();
		rootNode.put("state", state.toString());
		rootNode.put("phase", phase.toString());
		rootNode.put("age",Duration.ofMillis(Instant.now().toEpochMilli()-history.firstKey()).toString());
		rootNode.put("url", url);
		rootNode.put("seealso", "https://openspending.org/"+datasetName+".json");
		rootNode.put("download_progress_percent", downloadProgressPercent.toString());
		rootNode.put("convert_progress_percent", convertProgressPercent.toString());
		rootNode.put("upload_progress_percent", uploadProgressPercent.toString());
		ArrayNode operationsNode = mapper.createArrayNode();
		rootNode.put("operations", operationsNode);
		for(Operation op: operations.get(state)) {operationsNode.add(url+'/'+op.name().toLowerCase());}

		ArrayNode historyNode = mapper.createArrayNode();
		rootNode.put("history", historyNode);
		for(long epochMilli: history.keySet())
		{
			ObjectNode entryNode = mapper.createObjectNode();
			entryNode.put(Instant.ofEpochMilli(epochMilli).toString(), history.get(epochMilli).toString());
			historyNode.add(entryNode);
		}

		return rootNode;
	}
	/** returns whether all jobs are idle (not running). */
	public static boolean allIdle()
	{
		boolean busy = false;
		for(Job job:jobs.values()) {busy^=(job.getState()==RUNNING);}
		return !busy;
	}

}