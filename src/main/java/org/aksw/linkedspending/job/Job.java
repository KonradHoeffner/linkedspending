package org.aksw.linkedspending.job;

import static org.aksw.linkedspending.job.Job.State.*;
import static org.aksw.linkedspending.job.Job.Phase.*;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Represents the state of a dataset job, which can be conversion, download or upload. */
public class Job
{
	static final String PREFIX = "http://localhost:10010/";

	final String datasetName;

	public static enum State {CREATED,RUNNING,PAUSED,FINISHED,FAILED,STOPPED}
	private State state = CREATED;

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

	public State getState() {return state;}

	SortedMap<Long,State> history = new TreeMap<>();

	final long created = Instant.now().toEpochMilli();

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

	public Job(String datasetName)
	{
		this.datasetName = datasetName;
		this.phase=DOWNLOAD;
		history.put(Instant.now().toEpochMilli(), CREATED);
	}

	public ObjectNode json()
	{
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode();
		rootNode.put("state", state.toString());
		rootNode.put("phase", phase.toString());
		rootNode.put("age",Duration.ofMillis(Instant.now().toEpochMilli()-history.firstKey()).toString());
		rootNode.put("seealso", "https://openspending.org/"+datasetName+".json");

		ArrayNode operationsNode = mapper.createArrayNode();
		rootNode.put("operations", operationsNode);
		for(String op: operations.get(state)) {operationsNode.add(PREFIX+op);}

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

}