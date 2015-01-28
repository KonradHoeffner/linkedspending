package org.aksw.linkedspending.rest;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import lombok.extern.java.Log;
import org.aksw.linkedspending.LinkedSpendingDatasetInfo;
import org.aksw.linkedspending.OpenSpendingDatasetInfo;
import org.aksw.linkedspending.Virtuoso;
import org.aksw.linkedspending.exception.DataSetDoesNotExistException;
import org.aksw.linkedspending.job.Boss;
import org.aksw.linkedspending.job.Job;
import org.aksw.linkedspending.tools.PropertyLoader;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.konradhoeffner.commons.MemoryBenchmark;

@Path("")
@Log
public class Rest
{
	static final String PREFIX = PropertyLoader.apiUrl;
	static final int POOL_SIZE = 2;

	/** called by jersey on startup, initializes graph groups
	 */
	public Rest()
	{
		Virtuoso.createGraphGroup();
	}

	@GET @Path("listcommands")// @Produces("text/html")
	public static String listCommands()
	{
		String tableHead = "<table border=14>";

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
		boolean manual = args.length>0&&args[0].equals("manual");
		if(manual)
		{
			log.info("manual mode");
		}
		else
		{
			log.info("automatic mode");
			ScheduledExecutorService pool = Executors.newScheduledThreadPool(POOL_SIZE);
			for(int i=0;i<POOL_SIZE;i++)
			{
				pool.scheduleAtFixedRate(new Boss(),i,1,TimeUnit.MINUTES);
			}
		}
		GrizzlyHttpUtil.startThisServer();
		Thread.sleep(Duration.ofDays(10000).toMillis());
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

	private static String jobLink(String datasetName,boolean exists)
	{
		Job job = Job.jobs.get(datasetName);
		return "<a href=\""+Job.uriOf(datasetName)+(exists?"":"/create")+"\">"+(job==null?"create":job.getState())+"</a>";
	}

	//	static String state(String dataset)
	//	{
	//		if(jobs.containsKey(dataset)) return jobs.get(dataset).getState().toString();
	//		return "";
	//	}

	@GET @Path("datasets") @Produces(MediaType.TEXT_HTML)
	public static String datasets() throws IOException, DataSetDoesNotExistException
	{
		if(!OpenSpendingDatasetInfo.isOnline())
		{
			return "OpenSpending is offline!";
		}

		// TODO: this takes a long time to load, maybe some synchronization is in the way?
		Set<String> updateCandidates = new TreeSet<>();

		StringBuffer sb = new StringBuffer("<meta charset=\"UTF-8\"><html><body>");
		Map<String,LinkedSpendingDatasetInfo> lsInfos = LinkedSpendingDatasetInfo.all();

		//		sb.append("<table border=1><tr><th>dataset</th><th>status</th><th>added</th><th>job</th></tr>");
		StringBuffer tableSb = new StringBuffer();
		tableSb.append("Color Code Legend: ");
		tableSb.append("<span style='background-color:lightgreen'>converted and up to date</span> ");
		tableSb.append("<span style='background-color:lightblue'>converted and up to date but uses an old transformation model</span> ");
		tableSb.append("<span style='background-color:orange'>converted but outdated</span> ");
		tableSb.append("<span style='background-color:white'>not converted</span>");

		tableSb.append("<table border=1><tr><th>dataset</th><th>modified</th><th>created</th><th>source modified</th><th> source created</th><th>job</th><th>progress</th></tr>\n");
		SortedMap<String,OpenSpendingDatasetInfo> datasetInfos = OpenSpendingDatasetInfo.getDatasetInfosCached();
		for(String dataset: datasetInfos.keySet())
		{
			OpenSpendingDatasetInfo osInfo = datasetInfos.get(dataset);
			LinkedSpendingDatasetInfo lsInfo = lsInfos.get(dataset);
//
			final String tdModified;
			final String tdCreated;

			String color;
			if(lsInfo==null)
			{
				tdModified = "<td bgcolor=\"black\">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>";
				tdCreated = "<td bgcolor=\"black\">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>";
				color = "white";
			}
			else
			{
//								Instant modifiedInstant = Instant.ofEpochMilli(modified);
				if(lsInfo.modified.isAfter(osInfo.modified)&&LinkedSpendingDatasetInfo.newestTransformation(lsInfo.name))
				{
					color="lightgreen";
				}
				else
				{
					if(lsInfo.modified.isBefore(osInfo.modified)) {color="orange";}
					// transformation version changed
					else {color="lightblue";}

					// TODO what is this?
					updateCandidates.add(dataset);
				}
				tdModified = "<td bgcolor=\""+color+"\">"+lsInfo.modified+"</td>";
				tdCreated = "<td>"+lsInfo.created+"</td>";
			}

			// TODO increase performance by doing it all at once together with
			String tdSourceModified = "<td bgcolor='"+color+"'>"+osInfo.modified+"</td>";
			String tdSourceCreated = "<td>"+osInfo.modified+"</td>";

			String progress;
			String trStyle="";
			boolean jobExists=false;
			if(Job.all().contains(dataset))
			{
				jobExists=true;
				Job job = Job.forDatasetOrCreate(dataset);
				progress = job.downloadProgressPercent+"% | "+job.convertProgressPercent+"% | "+job.uploadProgressPercent+"%";

				switch(job.getState())
				{
					case RUNNING:trStyle="outline: thick solid #FF00FF;";break;
					case FINISHED:trStyle="outline: thick solid green;";break;
					case FAILED:trStyle="outline: thick solid red;";break;
					case PAUSED:trStyle="outline: thick solid yellow;";break;
					case STOPPED:trStyle="outline: thin solid red;";break;
					default: trStyle=trStyle="outline: thin solid lightgray;";
				}
			}
			else {progress = "";}
			//			//			String created = ?Instant.ofEpochMilli(sparqlDatasets.get(dataset)).toString():"";
			tableSb.append("<tr style='"+trStyle+"'><td>"+dataset+"</td>"+tdModified+tdCreated+tdSourceModified+tdSourceCreated+"</td><td>"+jobLink(dataset,jobExists)+"</td><td>"+progress+"</td></tr>\n");

			//			//			sb.append("<tr><td>"+dataset+"</td><td></td><td>"+jobLink(dataset)+"</td></tr>");
		}
//
		tableSb.append("</table>");

		// TODO implement selective mass operations
		//		sb.append("... <a href='...'>new datasets</a> on openspending <a href=''>convert them</a></br>\n");
		//		sb.append(updateCandidates.size()+" <a href='...'>modified datasets</a> on openspending <a href=''>update them</a></br>\n");
		//		sb.append("... <a href='...'>datasets with conversion errors</a> <a href=''>try them again</a></br>\n");

		sb.append(tableSb);

		sb.append("</body></html>");
		return sb.toString();
	}

	@GET @Path("") @Produces(MediaType.APPLICATION_JSON)
	public static String root() throws IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode();
		rootNode.put("doc", "https://github.com/AKSW/openspending2rdf/wiki/REST-API");
		rootNode.put("openspendingonline", OpenSpendingDatasetInfo.isOnline());
		rootNode.put("datasets", PREFIX+"datasets");
		rootNode.put("jobs", PREFIX+"jobs");
		rootNode.put("idle", Job.allIdle());
		rootNode.put("memory_mb", MemoryBenchmark.updateAndGetMemoryBytes()/1000_000);

		ArrayNode opNode = mapper.createArrayNode();
		//		// should shutdown and remove-all should be under admin
		opNode.add(PREFIX+"jobs/removeinactive");
		//		//		opNode.add(PREFIX+"remove-all");
		//		opNode.add(PREFIX+"process-all");
		//		opNode.add(PREFIX+"process-new");
		rootNode.put("operations",opNode);

		return rootNode.toString();
	}

}