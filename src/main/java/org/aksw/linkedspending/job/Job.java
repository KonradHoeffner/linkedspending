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
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import lombok.extern.java.Log;
import org.aksw.linkedspending.OpenSpendingDatasetInfo;
import org.aksw.linkedspending.exception.DataSetDoesNotExistException;
import org.aksw.linkedspending.tools.PropertyLoader;
import com.fasterxml.jackson.databind.JsonNode;
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

	// convert, download and upload even if already existing
	static private final boolean	FORCE	= false;

	static final String PREFIX = PropertyLoader.apiUrl+"jobs/";

	final public String url;
	final String datasetName;

	private State state = CREATED;
	private Phase phase = DOWNLOAD;

	Optional<DownloadConvertUploadWorker> worker = Optional.empty();

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

	static public Set<String> all()
	{
		return new HashSet<>(jobs.keySet());
	}

	@GET @Path("") @Produces("application/json")
	public static String jobs() throws IOException
	{
		//		jobs.add(new Job(""+new Random().nextInt()));
		ObjectMapper mapper = new ObjectMapper();
		ArrayNode rootNode = mapper.createArrayNode();

		for(Job job:Job.jobs.values()) {rootNode.add(job.json());}

		return rootNode.toString();
	}

	@GET @Path("/removeinactive") @Produces(MediaType.TEXT_HTML)
	public static String removeInactive()
	{
		synchronized(jobs)
		{
			Set<Job> inactive = jobs.values().stream().filter(j->j.getOperations().contains(Operation.REMOVE)).collect(Collectors.toSet());
			inactive.stream().forEach(Job::terminate);
			jobs.values().removeAll(inactive);
			return String.valueOf(!inactive.isEmpty());
		}
	}

	private static String jsonMessage(String message,String... links)
	{
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		node.put("message", message);
		if(links.length>0)
		{
			ArrayNode linkNode = mapper.createArrayNode();
			node.put("links", linkNode);
			for(String link: links) {linkNode.add(link);}
		}
		return node.toString();
	}

	@GET @Path("/{datasetname}") @Produces(MediaType.APPLICATION_JSON)
	public static String jsonForDataset(@PathParam("datasetname") String datasetName) //throws DataSetDoesNotExistException
	{
		try
		{
			// TODO if this ever gets used by machines and more time available, error message should be in json content type as well
			return Job.forDataset(datasetName).map(Job::json).map(ObjectNode::toString)
					.orElse(jsonMessage("Job for dataset "+datasetName+" does not exist.",PREFIX+datasetName+"/create"));
		}
		catch(DataSetDoesNotExistException e) {return e.getMessage();}
	}

	@GET @Path("/{datasetname}/{operation}") @Produces(MediaType.APPLICATION_JSON)
	public static String operation(@PathParam("datasetname") String datasetName,@PathParam("operation") String operationName) throws InterruptedException
	{
		String jobLink = PREFIX+datasetName;
		try
		{
			Operation op = Operation.valueOf(operationName.toUpperCase());
			try
			{
				Optional<Job> jobo = Job.forDataset(datasetName);
				if(op==Operation.CREATE)
				{
					String uri = uriOf(datasetName);
					String messagePrefix = "job for dataset "+datasetName;
					if(jobo.isPresent()) return jsonMessage(messagePrefix+" already exists",jobLink);
					forDatasetOrCreate(datasetName);
					String startUri=uri+"/start";
					return jsonMessage(messagePrefix+" successfully created, but not started yet",jobLink,jobLink+"/start");
				}
				Job job = jobo.get();
				if(!job.getOperations().contains(op))
				{return jsonMessage("operation \""+operationName+"\" not possible in state \""+job.state+"\". Nothing done.",jobLink);}

				switch(op)
				{
					case START:return jsonMessage("Starting successful: "+String.valueOf(job.start()),jobLink);
					case STOP:		job.worker.ifPresent(Worker::stop);break;
					case PAUSE:	job.worker.ifPresent(Worker::pause);break;
					case RESUME:	job.worker.ifPresent(Worker::resume);break;
					case REMOVE:
						synchronized(jobs)
						{
							job.terminate();
							jobs.remove(datasetName);
							break;
						}
					default: return jsonMessage("todo: operation "+op+" on dataset "+datasetName,jobLink);
				}
				return jsonMessage("executed operation "+op+" on job for dataset "+datasetName,jobLink);
			}
			catch(DataSetDoesNotExistException e) {return jsonMessage(e.getMessage(),jobLink);}
		}
		catch(IllegalArgumentException e)
		{
			return jsonMessage("operation \""+operationName+"\" does not exist. Nothing done.",jobLink);
		}
	}


	private void terminate()
	{
		worker.ifPresent(Worker::stop);
		worker=Optional.empty();
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

	public static Job forDatasetOrCreate(String datasetName) throws DataSetDoesNotExistException
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

	public static Optional<Job> forDataset(String datasetName) throws DataSetDoesNotExistException
	{
		if(!OpenSpendingDatasetInfo.getDatasetInfosCached().keySet().contains(datasetName)) throw new DataSetDoesNotExistException(datasetName);
		synchronized(jobs)
		{
			return Optional.ofNullable(jobs.get(datasetName));
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
		s.put(CREATED, EnumSet.of(START,STOP,REMOVE));
		s.put(RUNNING, EnumSet.of(STOP,PAUSE));
		s.put(PAUSED, EnumSet.of(STOP,RESUME));

		s.put(STOPPED, EnumSet.of(REMOVE));
		s.put(FINISHED, EnumSet.of(REMOVE));
		s.put(FAILED, EnumSet.of(REMOVE));
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
				worker = Optional.of(new DownloadConvertUploadWorker (datasetName,this,FORCE));
				CompletableFuture.supplyAsync(worker.get());
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

	@Override public String toString()
	{
		return datasetName;
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