package org.aksw.linkedspending.job;

import static org.aksw.linkedspending.job.Job.Operation.*;
import static org.aksw.linkedspending.job.Job.State.*;
import static org.aksw.linkedspending.job.Job.Phase.*;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import lombok.extern.java.Log;
import org.aksw.linkedspending.downloader.JsonDownloader;
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

	/** dont use it, its for jersey */
	public Job() {datasetName="dummyForJersey";this.phase=DOWNLOAD;this.url=uriOf(datasetName);}

	static public class DataSetDoesNotExistException extends Exception
	{
		public DataSetDoesNotExistException(String datasetName)
		{
			super("dataset \""+datasetName+"\" is not present at OpenSpending (in case of old cache, refresh it).");
		}
	}

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

	private Job(String datasetName)
	{
//		future.ex
		this.datasetName = datasetName;
		this.phase=DOWNLOAD;
		this.url=uriOf(datasetName);
		//		this.prefix=url+'/';
		history.put(Instant.now().toEpochMilli(), CREATED);
	}

	static
	{
		jobs.put("orcamento_publico",new Job("orcamento_publico"));
	}

	synchronized public static Job forDataset(String datasetName) throws DataSetDoesNotExistException
	{
		if(!JsonDownloader.getDatasetInfosCached().keySet().contains(datasetName)) throw new DataSetDoesNotExistException(datasetName);
		synchronized(jobs)
		{
			Job job = jobs.get(datasetName);
			if(job==null) {job=new Job(datasetName);}
			return job;
		}
	}

	static final String ROOT_PREFIX = "http://localhost:10010/";
	static final String PREFIX = ROOT_PREFIX+"jobs/";
	final public String url;
	//	final String prefix;

	final String datasetName;


	public static enum Operation {START,STOP,PAUSE,RESUME}

	public static enum State {CREATED,RUNNING,PAUSED,FINISHED,FAILED,STOPPED}
	private State state = CREATED;

	//	String errorMessage = "";
	//	public void setErrorMessage(String errorMessage) {this.errorMessage=errorMessage;}

	static final Map<State,EnumSet<State>> transitions;
	static
	{
		Map<State,EnumSet<State>> t = new HashMap<>();

		t.put(CREATED,EnumSet.of(RUNNING,STOPPED));
		t.put(RUNNING,EnumSet.of(PAUSED,FINISHED,FAILED,STOPPED));
		t.put(PAUSED,EnumSet.of(RUNNING,STOPPED));
		t.put(FINISHED, EnumSet.noneOf(Job.State.class));
		t.put(FAILED, EnumSet.noneOf(Job.State.class));
		t.put(STOPPED, EnumSet.noneOf(Job.State.class));
		transitions = Collections.unmodifiableMap(t);
	}

	//	static SortedMap<Pair<State>,String> operationNames = new TreeMap<>();
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

		//		SortedMap<Pair<State>,String> s = new TreeMap<>();
		//		s.put(new Pair<>(CREATED,RUNNING), "start");
		//		s.put(new Pair<>(CREATED,STOPPED), "stop");
		//		s.put(new Pair<>(RUNNING,STOPPED), "stop");
		//		s.put(new Pair<>(PAUSED,STOPPED), "stop");
		//		s.put(new Pair<>(RUNNING,PAUSED), "pause");
		//		s.put(new Pair<>(PAUSED,RUNNING), "resume");
		operations = Collections.unmodifiableSortedMap(s);
	}

	public EnumSet getOperations() {return operations.get(state);}

	public enum Phase {DOWNLOAD,CONVERT,UPLOAD}
	private Phase phase;

	public Phase getPhase() {return phase;}
	public State getState() {return state;}

	SortedMap<Long,State> history = new TreeMap<>();

	final long created = Instant.now().toEpochMilli();

	public static String uriOf(String datasetName) {return PREFIX+datasetName;}

	public boolean setState(State newState)
	{
		if(transitions.get(state).contains(newState))
		{
			history.put(Instant.now().toEpochMilli(), newState);
			state=newState;
			return true;
		}
		return false;
	}

	public boolean nextPhase()
	{
		if(state!=RUNNING) {return false;}
		switch(phase)
		{
			case DOWNLOAD: phase=CONVERT;break;
			case CONVERT: phase=UPLOAD;
		}
		return true;
	}

	public synchronized boolean start() throws InterruptedException
	{
		if(setState(RUNNING))
		{
			try
			{
//				future = CompletableFuture.runAsync(this:downloadConvertUpload);
			}
			catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return true;
		}
		return false;
	}

	private void downloadConvertUpload() throws IOException, InterruptedException, ExecutionException
	{
		JsonDownloader.downloadSpecificOld(datasetName);
		phase=CONVERT;
	}



	//	public boolean pause()
	//	{
	//
	//	}
	//
//		public boolean stop()
//		{
//
//		}
	//
	//	public boolean resume()
	//	{
	//
	//	}

	public ObjectNode json()
	{
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode();
		rootNode.put("state", state.toString());
		rootNode.put("phase", phase.toString());
		rootNode.put("age",Duration.ofMillis(Instant.now().toEpochMilli()-history.firstKey()).toString());
		rootNode.put("url", url);
		rootNode.put("seealso", "https://openspending.org/"+datasetName+".json");

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

	//	public static void main(String[] args) throws DataSetDoesNotExistException
	//	{
	//		System.out.println(jobs);
	//		System.out.println(job("berlin_de"));
	//	}

}