package org.aksw.linkedspending.rest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.aksw.linkedspending.DatasetInfo;
import org.aksw.linkedspending.job.Job;
import org.aksw.linkedspending.old.JsonDownloaderOld;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.konradhoeffner.commons.MemoryBenchmark;

@Path("")
public class Rest
{

	static final String PREFIX = "http://localhost:10010/";
	//
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
		//		System.out.println(datasets());
		GrizzlyHttpUtil.startThisServer();
		Thread.sleep(Duration.ofDays(10000).toMillis());
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

	private static String jobLink(String datasetName)
	{
		Job job = Job.jobs.get(datasetName);
		return "<a href=\""+Job.uriOf(datasetName)+"\">"+(job==null?"create":job.getState())+"</a>";
	}

	//	static String state(String dataset)
	//	{
	//		if(jobs.containsKey(dataset)) return jobs.get(dataset).getState().toString();
	//		return "";
	//	}

	@GET @Path("datasets") @Produces(MediaType.TEXT_HTML)
	public static String datasets() throws IOException
	{
		Set<String> updateCandidates = new TreeSet<>();

		StringBuffer sb = new StringBuffer("<meta charset=\"UTF-8\"><html><body>");
		Map<String,Long> sparqlDatasets = Sparql.datasetsByName();

		//		sb.append("<table border=1><tr><th>dataset</th><th>status</th><th>added</th><th>job</th></tr>");
		StringBuffer tableSb = new StringBuffer();
		tableSb.append("<table border=1><tr><th>dataset</th><th>converted</th><th>modified</th><th>job</th></tr>\n");
		SortedMap<String,DatasetInfo> datasetInfos = JsonDownloaderOld.getDatasetInfosCached();
		for(String dataset: datasetInfos.keySet())
		{
			DatasetInfo datasetInfo = datasetInfos.get(dataset);
			String converted;
			Long time = sparqlDatasets.get(dataset);
			String color;
			if(time==null)
			{
				converted = "<td bgcolor=\"black\">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>";
				color = "white";
			}
			else
			{
				Instant convertedInstant = Instant.ofEpochMilli(time);
				if(convertedInstant.isBefore(datasetInfo.modified))
				{
					color="orange";
					updateCandidates.add(dataset);
				}
				else
				{
					color="lightgreen";
				}
				converted = "<td bgcolor=\""+color+"\">"+convertedInstant+"</td>";
			}

			String modified = "<td bgcolor=\""+color+"\">"+datasetInfo.modified+"</td>";

			//			//			String created = ?Instant.ofEpochMilli(sparqlDatasets.get(dataset)).toString():"";
			tableSb.append("<tr><td>"+dataset+"</td>"+converted+modified+"</td><td>"+jobLink(dataset)+"</td></tr>\n");
			//			//			sb.append("<tr><td>"+dataset+"</td><td></td><td>"+jobLink(dataset)+"</td></tr>");
		}

		tableSb.append("</table>");

		sb.append("... <a href='...'>new datasets</a> on openspending <a href=''>convert them</a></br>\n");
		sb.append(updateCandidates.size()+" <a href='...'>modified datasets</a> on openspending <a href=''>update them</a></br>\n");
		sb.append("... <a href='...'>datasets with conversion errors</a> <a href=''>try them again</a></br>\n");

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
		rootNode.put("datasets", PREFIX+"datasets");
		rootNode.put("jobs", PREFIX+"jobs");
		rootNode.put("idle", Job.allIdle());
		rootNode.put("memory_mb", MemoryBenchmark.updateAndGetMemoryBytes()/1000_000);

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