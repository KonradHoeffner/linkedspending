package org.aksw.linkedspending;

import static org.aksw.linkedspending.job.Job.State.RUNNING;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import org.aksw.linkedspending.job.Job;
import org.aksw.linkedspending.job.TestJob;
import org.aksw.linkedspending.tools.GrizzlyHttpUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/")
public class Rest
{
	static Set<Job> jobs = new HashSet<>();
	{
		jobs.add(new TestJob());
	}
	static final String PREFIX = "http://localhost:10010/";
//
	@GET @Path("listcommands")// @Produces("text/html")
	public static String listCommands()
	{
		String tableHead = "<table border=1>";

		tableHead += "<th>command</th>";
		tableHead += "<th>description</th>";
		String tr1 = "<tr><td>/control/runmanually</td><td>Enables manual mode required for use of several commands.</td></tr>";
		String tr2 = "<tr><td>/control/runautomatically</td><td>Enables automatic mode. Complete runs will be performed according to a specified time table.</td></tr>";
		String tr3 = "<tr><td>/control/setstartdate/{day}/{hour}/{repeat}</td><td>Configures time table of complete runs. Allowed parameters are day: 1-Monday...7-Sunday, hour-1...24, repeat: number of weeks to repeat complete runs</td></tr>";
		String tr4 = "<tr><td>/control/downloadcomplete</td><td>Starts a complete download of all available datasets. Requires manual mode.</td></tr>";
		String tr5 = "<tr><td>/control/stopdownload</td><td>Stops current downloading process and deletes unfinished datasets. Requires manual mode.</td></tr>";
		String tr6 = "<tr><td>/control/pausedownload</td><td>Pauses current downloading process to be resumed later. Requires manual mode.</td></tr>";
		String tr7 = "<tr><td>/control/resumedownload</td><td>Resumes current downloading process. Has no effect if downloader is unpaused. Requires manual mode.</td></tr>";
		String tr8 = "<tr><td>/control/downloadcomplete</td><td>Starts a complete download of all available datasets. Requires manual mode.</td></tr>";
		String tr9 = "<tr><td>/control/downloadspecific/{param}</td><td>Downloads a single dataset whose name is specified in {param}. Requires manual mode.</td></tr>";
		String tr10 = "<tr><td>/control/convertcomplete</td><td>Converts all new datasets. Requires manual mode.</td></tr>";
		String tr11 = "<tr><td>/control/stopconvert</td><td>Stops the current converting process. Requires manual mode.</td></tr>";
		String tr12 = "<tr><td>/control/pauseconvert</td><td>Pauses the current converting process to be resumed later. Requires manual mode.</td></tr>";
		String tr13 = "<tr><td>/control/resumeconvert</td><td>Converts all new datasets. Requires manual mode.</td></tr>";
		String tr14 = "<tr><td>/control/shutdown</td><td>Completly shuts down the program.</td></tr>";
		String tr15 = "<tr><td>/control/listcommands</td><td>Displays a table containing all available commands.</td></tr>";

		String tableEnd = "</table>";

		return tableHead + tr1 + tr2 + tr3 + tr4 + tr5 + tr6 + tr7 + tr8 + tr9 + tr10 + tr11 + tr12 + tr13 + tr14 + tr15
				+ tableEnd;
	}

	public static void main(String[] args) throws IOException, InterruptedException
	{
		GrizzlyHttpUtil.startThisServer();
		Thread.sleep(100000);
//		root();
	}

//	/** Completly shutdowns downloader, converter (if running) and scheduler */
//	@GET @Path("shutdown") public static void shutdown()
//	{
////		shutdownRequested = true;
////		if (downloaderThread != null) stopDownloader();
////		if (converterThread != null) stopConverter();
//		GrizzlyHttpUtil.shutdownGrizzly();
//		System.exit(0);
//		return;
//	}

	@GET @Path("jobs") @Produces("application/json")
	public static String jobs() throws IOException
	{
//		jobs.add(new Job(""+new Random().nextInt()));
		ObjectMapper mapper = new ObjectMapper();
		ArrayNode rootNode = mapper.createArrayNode();

		for(Job job:jobs) {rootNode.add(job.json());}

		return rootNode.toString();
	}

	@GET @Path("") @Produces("application/json")
	public static String root() throws IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode();
		rootNode.put("doc", "https://github.com/AKSW/openspending2rdf/wiki/REST-API");
		rootNode.put("jobs", PREFIX+"jobs");
		boolean busy = false;
		for(Job job:jobs) {busy^=(job.getState()==RUNNING);}
		rootNode.put("idle", !busy);

		ArrayNode opNode = mapper.createArrayNode();
		// should shutdown and remove-all should be under admin
//		opNode.add(PREFIX+"shutdown");
//		opNode.add(PREFIX+"remove-all");
		opNode.add(PREFIX+"process-all");
		opNode.add(PREFIX+"process-new");

		rootNode.put("operations",opNode);


		return rootNode.toString();
	}

}