//package org.aksw.linkedspending.job;
//
//import java.io.IOException;
//import java.util.Collections;
//import java.util.EnumSet;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.SortedMap;
//import java.util.TreeMap;
//import javax.inject.Singleton;
//import javax.ws.rs.GET;
//import javax.ws.rs.Path;
//import javax.ws.rs.PathParam;
//import javax.ws.rs.Produces;
//import javax.ws.rs.core.MediaType;
//import lombok.extern.java.Log;
//import org.aksw.linkedspending.downloader.JsonDownloader;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ArrayNode;
//import static org.aksw.linkedspending.job.State.*;
//import static org.aksw.linkedspending.job.Phase.*;
//import static org.aksw.linkedspending.job.Operation.*;
//
///** Static class that manages the jobs. */
//@Log
//@Singleton
//@Path("/jobs")
//public class Jobs
//{
//	static public Map<String,Job> jobs = new HashMap<>();
//
//	@GET @Path("") @Produces("application/json")
//	public static String jobs() throws IOException
//	{
//		ObjectMapper mapper = new ObjectMapper();
//		ArrayNode rootNode = mapper.createArrayNode();
//
//		for(Job job: jobs.values()) {rootNode.add(job.json());}
//
//		return rootNode.toString();
//	}
//
//	@GET @Path("/{datasetname}") @Produces(MediaType.APPLICATION_JSON)
//	public static String json(@PathParam("datasetname") String datasetName) //throws DataSetDoesNotExistException
//	{
//		try{return Jobs.forDataset(datasetName).json().toString();}
//		catch(DataSetDoesNotExistException e) {return e.getMessage();}
//	}
//
//	@GET @Path("/{datasetname}/{operation}") @Produces(MediaType.TEXT_PLAIN)
//	public static String operation(@PathParam("datasetname") String datasetName,@PathParam("operation") String operationName) throws InterruptedException
//	{
//		try
//		{
//			Job job = Jobs.forDataset(datasetName);
//			try
//			{
//				Operation op = Operation.valueOf(operationName.toUpperCase());
//				if(!job.getOperations().contains(op)) {return "operation \""+operationName+"\" not possible in state \""+job.getState()+"\". Nothing done.";}
//				switch(op)
//				{
//					case START:job.start();
//				}
//				return "todo: operation "+op+" on dataset "+datasetName;
//			}
//			catch(IllegalArgumentException e)
//			{
//				return "operation \""+operationName+"\" does not exist. Nothing done.";
//			}
//		}
//		catch(DataSetDoesNotExistException e) {return e.getMessage();}
//	}
//
//	synchronized public static Job forDataset(String datasetName) throws DataSetDoesNotExistException
//	{
//		if(!datasetExists(datasetName)) throw new DataSetDoesNotExistException(datasetName);
//		synchronized(jobs)
//		{
//			Job job = jobs.get(datasetName);
//			if(job==null) {job=new Job(datasetName);}
//			return job;
//		}
//	}
//
//	private static boolean datasetExists(String datasetName)
//	{
//		return JsonDownloader.getDatasetInfosCached().keySet().contains(datasetName);
//	}
//
//	static final String ROOT_PREFIX = "http://localhost:10010/";
//	static final String PREFIX = ROOT_PREFIX+"jobs/";
//
//	//	String errorMessage = "";
//	//	public void setErrorMessage(String errorMessage) {this.errorMessage=errorMessage;}
//
//	static final Map<State,EnumSet<State>> transitions;
//	static
//	{
//		Map<State,EnumSet<State>> t = new HashMap<>();
//
//		t.put(CREATED,EnumSet.of(RUNNING,STOPPED));
//		t.put(RUNNING,EnumSet.of(PAUSED,FINISHED,FAILED,STOPPED));
//		t.put(PAUSED,EnumSet.of(RUNNING,STOPPED));
//		t.put(FINISHED, EnumSet.noneOf(State.class));
//		t.put(FAILED, EnumSet.noneOf(State.class));
//		t.put(STOPPED, EnumSet.noneOf(State.class));
//		transitions = Collections.unmodifiableMap(t);
//	}
//
//	//	static SortedMap<Pair<State>,String> operationNames = new TreeMap<>();
//	static SortedMap<State,EnumSet<Operation>> operations = new TreeMap<>();
//	static
//	{
//		SortedMap<State,EnumSet<Operation>> s = new TreeMap<>();
//		s.put(CREATED, EnumSet.of(START,STOP));
//		s.put(RUNNING, EnumSet.of(STOP,PAUSE));
//		s.put(PAUSED, EnumSet.of(STOP,RESUME));
//
//		s.put(STOPPED, EnumSet.noneOf(Operation.class));
//		s.put(FINISHED, EnumSet.noneOf(Operation.class));
//		s.put(PAUSED, EnumSet.noneOf(Operation.class));
//
//		//		SortedMap<Pair<State>,String> s = new TreeMap<>();
//		//		s.put(new Pair<>(CREATED,RUNNING), "start");
//		//		s.put(new Pair<>(CREATED,STOPPED), "stop");
//		//		s.put(new Pair<>(RUNNING,STOPPED), "stop");
//		//		s.put(new Pair<>(PAUSED,STOPPED), "stop");
//		//		s.put(new Pair<>(RUNNING,PAUSED), "pause");
//		//		s.put(new Pair<>(PAUSED,RUNNING), "resume");
//		operations = Collections.unmodifiableSortedMap(s);
//	}
//
//	public static String uriOf(String datasetName) {return PREFIX+datasetName;}
//
//	/** returns whether all jobs are idle (not running). */
//	public static boolean allIdle()
//	{
//		boolean busy = false;
//		for(Job job:jobs.values()) {busy^=(job.getState()==RUNNING);}
//		return !busy;
//	}
//
//}