package org.aksw.linkedspending.job;

import static org.aksw.linkedspending.job.Job.Phase.CONVERT;
import static org.aksw.linkedspending.job.Job.Phase.DOWNLOAD;
import static org.aksw.linkedspending.job.Job.Phase.UPLOAD;
import static org.aksw.linkedspending.job.Job.State.CREATED;
import static org.aksw.linkedspending.job.Job.State.FAILED;
import static org.aksw.linkedspending.job.Job.State.FINISHED;
import static org.aksw.linkedspending.job.Job.State.PAUSED;
import static org.aksw.linkedspending.job.Job.State.RUNNING;
import static org.aksw.linkedspending.job.Job.State.STOPPED;
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
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.aksw.linkedspending.downloader.JsonDownloader;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Represents the state of a dataset job, which can be conversion, download or upload. */
@Singleton
@Path("jobs")
public class Job
{
	/** dont use it, its for jersey */
	public Job() {datasetName="dummyForJersey";this.phase=DOWNLOAD;this.url=uriOf(datasetName);}

	static public class DataSetDoesNotExistException extends Exception
	{
		public DataSetDoesNotExistException(String datasetName)
		{
			super("dataset \""+datasetName+"\" is not present at OpenSpending (in case of old cache, call ).");
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

	@GET @Path("{datasetname}") @Produces(MediaType.APPLICATION_JSON)
	public static String job(@PathParam("datasetname") String datasetName) //throws DataSetDoesNotExistException
	{
		try{return Job.forDataset(datasetName).json().toString();}
		catch(DataSetDoesNotExistException e) {return e.getMessage();}
	}

	private Job(String datasetName)
	{
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
		Job job = jobs.get(datasetName);
		if(job==null) {job=new Job(datasetName);}
		return job;
	}

	static final String ROOT_PREFIX = "http://localhost:10010/";
	static final String PREFIX = ROOT_PREFIX+"jobs/";
	final public String url;
//	final String prefix;

	final String datasetName;

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
	static SortedMap<State,List<String>> operations = new TreeMap<>();
	static
	{
		SortedMap<State,List<String>> s = new TreeMap<>();
		s.put(CREATED, Arrays.asList("start","stop"));
		s.put(RUNNING, Arrays.asList("stop","pause"));
		s.put(PAUSED, Arrays.asList("stop","resume"));

		s.put(STOPPED, Collections.emptyList());
		s.put(FINISHED, Collections.emptyList());
		s.put(PAUSED, Collections.emptyList());

		//		SortedMap<Pair<State>,String> s = new TreeMap<>();
		//		s.put(new Pair<>(CREATED,RUNNING), "start");
		//		s.put(new Pair<>(CREATED,STOPPED), "stop");
		//		s.put(new Pair<>(RUNNING,STOPPED), "stop");
		//		s.put(new Pair<>(PAUSED,STOPPED), "stop");
		//		s.put(new Pair<>(RUNNING,PAUSED), "pause");
		//		s.put(new Pair<>(PAUSED,RUNNING), "resume");
		operations = Collections.unmodifiableSortedMap(s);
	}

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

	public boolean start()
	{
		if(setState(RUNNING))
		{
			startDownload();
			return true;
		}
		return false;
	}

	private void startDownload()
	{

	}

	//	public boolean pause()
	//	{
	//
	//	}
	//
	//	public boolean stop()
	//	{
	//
	//	}
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
//		for(String op: operations.get(state)) {operationsNode.add(prefix+op);}

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