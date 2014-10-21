package org.aksw.linkedspending.scheduler;

import java.net.URI;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.UriBuilder;
import org.aksw.linkedspending.OpenspendingSoftwareModule;
import org.aksw.linkedspending.converter.Converter;
import org.aksw.linkedspending.downloader.JsonDownloader;
import org.aksw.linkedspending.tools.ConverterSleeper;
import org.aksw.linkedspending.tools.GrizzlyHttpUtil;

/**
 * Pre-version of planned scheduler, which can (directly) control the JsonDownloader
 * (start/stop/pause, all/specified datasets).
 */
@Path("control") public class Scheduler
{
	public static final URI				baseURI		= UriBuilder.fromUri("http://localhost/").port(10010).build();

	private static Thread				scheduleTimeThread;
	private static Thread				downloaderThread;
	private static Thread				converterThread;
	private static JsonDownloader		downloader	= new JsonDownloader();
	private static Converter			converter	= new Converter();
	private static ScheduleTimeHandler	scheduleTimeHandler;

	public static URI getBaseURI()
	{
		return baseURI;
	}

	protected static Thread getDownloaderThread()
	{
		return downloaderThread;
	}

	protected static Thread getConverterThread()
	{
		return converterThread;
	}

	public static JsonDownloader getDownloader()
	{
		return downloader;
	}

	public static Converter getConverter()
	{
		return converter;
	}

	private static boolean	shutdownRequested	= false;
	private static boolean	manualMode			= false;

	private static void shutdownScheduleTimeHandler()
	{
		ScheduleTimeHandler.setShutdownRequested(true);
	}

	/**
	 * Sets a new start date for scheduled runs (e.g. setstartdate/7/20 sets start time to 20
	 * o'clock at Sunday)
	 *
	 * @param startDay
	 *            The starting day (1 = Monday, ... , 7 = Sunday)
	 * @param startHour
	 *            The starting hour
	 * @param repeat
	 *            The ratio to repeat in weeks (e.g. 2 means one run every two weeks)
	 */
	@Path("setstartdate/{day}/{hour}/{repeat}") public static String setScheduleTime(@PathParam("day") int startDay,
			@PathParam("hour") int startHour, @PathParam("repeat") int repeat)
	{
		ScheduleTimeHandler.setShutdownRequested(true);

		scheduleTimeHandler.setStartDay(startDay);
		scheduleTimeHandler.setStartTime(startHour);
		scheduleTimeHandler.setRepeat(repeat);

		scheduleTimeThread = new Thread(scheduleTimeHandler);
		scheduleTimeThread.start();
		return "Set new start date!";
	}

	/**
	 * For running the programm manually. ScheduleTimeHandler will be stopped and the programm will
	 * await commands from a user.
	 */
	@GET @Path("runmanually") public static String runManually()
	{
		manualMode = true;
		shutdownScheduleTimeHandler();
		return "Manual running has been started.";
	}

	/**
	 * Resets the programm to run in automatic mode after it has been run manually.
	 * Has no effect if it hasn't been run manually yet.
	 */
	@GET @Path("runautomatically") public static String runAutomatically()
	{
		if (!manualMode) return "Programm already running automatically.";
		else
		{
			manualMode = false;
			ScheduleTimeHandler.setShutdownRequested(false);
			scheduleTimeThread = new Thread(scheduleTimeHandler);
			scheduleTimeThread.start();
			return "Program now is in automatic mode.";
		}
	}

	/**
	 * Starts complete download.
	 * To be used for out-of-schedule runs only.
	 */
	@GET @Path("downloadcomplete") public static String runDownloader()
	{
		if (manualMode)
		{
			OpenspendingSoftwareModule.setStopRequested(false);
			JsonDownloader.setPauseRequested(false);
			JsonDownloader.setCompleteRun(true);
			downloaderThread = new Thread(downloader);
			downloaderThread.start();
			return "Started complete download";
		}
		else return "Error: Program not in manual mode!";
	}

	/** Stops JsonDownloader. Already started downloads won't be finished, use it carefully! */
	@GET @Path("stopdownload") public static String stopDownloader()
	{
		if (manualMode)
		{
			OpenspendingSoftwareModule.setStopRequested(true);
			return "Stopped downloading";
		}
		else return "Error: Program not in manual mode!";
	}

	/** Pauses JsonDownloader */
	@GET @Path("pausedownload") public static String pauseDownloader()
	{
		if (manualMode)
		{
			JsonDownloader.setPauseRequested(true);
			return "Paused Downloader";
		}
		else return "Error: Program not in manual mode!";
	}

	/** Resumes downloading process */
	@GET @Path("resumedownload") public static String resumeDownload()
	{
		if (manualMode)
		{
			JsonDownloader.setPauseRequested(false);
			return "Resumed Downloader";
		}
		else return "Error: Program not in manual mode!";
	}

	/** Starts downloading a specified dataset */
	@GET @Path("downloadspecific/{param}") public static String downloadDataset(@PathParam("param") String datasetName)
	{
		if (manualMode)
		{
			JsonDownloader.setCompleteRun(false);
			JsonDownloader.setToBeDownloaded(datasetName);
			downloaderThread = new Thread(downloader);
			downloaderThread.start();
			return "Started downloading dataset " + datasetName;
		}
		else return "Error: Program not in manual mode!";
	}

	/**
	 * Starts converting of all new Datasets.
	 * To be used for out-of-schedule runs only.
	 */
	@GET @Path("convertcomplete") public static String runConverter()
	{
		if (manualMode)
		{
			OpenspendingSoftwareModule.setPauseRequested(false);
			OpenspendingSoftwareModule.setStopRequested(false);
			converterThread = new Thread(converter);
			converterThread.start();
			return "Started Converter.";
		}
		else return "Error: Program not in manual mode!";
	}

	/** Stops the converting process */
	@GET @Path("stopconvert") public static String stopConverter()
	{
		if (manualMode)
		{
			// converterThread.interrupt();
			OpenspendingSoftwareModule.setStopRequested(true);
			return "Stopped Converter.";
		}
		else return "Error: Program not in manual mode!";
	}

	/** Pauses converting process */
	@GET @Path("pauseconvert") public static String pauseConverter()
	{
		if (manualMode)
		{
			OpenspendingSoftwareModule.setPauseRequested(true);
			ConverterSleeper cS = new ConverterSleeper();
			Thread cSThread = new Thread(cS);
			cSThread.start();
			return "Paused Converter.";
		}
		else return "Error: Program not in manual mode!";
	}

	/** Resumes converting process */
	@GET @Path("resumeconvert") public static String resumeConverter()
	{
		if (manualMode)
		{
			OpenspendingSoftwareModule.setPauseRequested(false);
			return "Resumed Converter";
		}
		else return "Error: Program not in manual mode!";
	}

	/** Completly shutdowns downloader, converter (if running) and scheduler */
	@GET @Path("shutdown") public static String shutdown()
	{
		shutdownRequested = true;
		if (downloaderThread != null) stopDownloader();
		if (converterThread != null) stopConverter();
		GrizzlyHttpUtil.shutdownGrizzly();
		System.exit(0);
		return "Service shut down.";
	}

	/**
	 * Runs a complete download after a specified period of time and starts converting afterwards.
	 * To be used for out-of-schedule runs only.
	 *
	 * @Param timeTillStart the specified point of time
	 * @Param unit the unit of time measurement (d for days, min for minutes)
	 */
	@GET @Path("schedule/{time}/{unit}") public static String scheduleCompleteRun(@PathParam("time") int timeTillStart,
			@PathParam("unit") String unit)
	{
		if (manualMode)
		{
			long timeInMs;
			if (unit.equals("d")) timeInMs = timeTillStart * 1000 * 60 * 60 * 24;
			else if (unit.equals("h")) timeInMs = timeTillStart * 1000 * 60 * 60;
			else if (unit.equals("min")) timeInMs = timeTillStart * 1000 * 60;
			else if (unit.equals("s")) timeInMs = timeTillStart * 1000;
			else return "Wrong arguments.";

			scheduleCompleteRun(timeInMs);
			return "Complete run starting in " + timeTillStart + unit;
		}
		else return "Error: Program not in manual mode!";
	}

	/**
	 * Helping method for String scheduleCompleteRun(). Waits specified time and runs downloader and
	 * converter.
	 * Converter is run after download has finished.
	 */
	private static void scheduleCompleteRun(long timeInMs)
	{
		try
		{
			Thread.sleep(timeInMs);
		}
		catch (InterruptedException e)
		{}

		runDownloader();

		OpenspendingSoftwareModule.setPauseRequested(false);
		OpenspendingSoftwareModule.setStopRequested(false);
		converterThread = new Thread(converter);

		ConverterSleeper cS = new ConverterSleeper();
		Thread cSThread = new Thread(cS);
		converterThread.start();
		cSThread.start();
	}

	/**
	 * Returns a html table which lists all available commands and a description for each of them.
	 *
	 * @return html-formatted String
	 */
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

	public static void main(String[] args)
	{
		// ScheduleTimeHandler sth = new ScheduleTimeHandler();
		scheduleTimeHandler = new ScheduleTimeHandler();
		scheduleTimeThread = new Thread(scheduleTimeHandler);
		scheduleTimeThread.start();

		try
		{
			GrizzlyHttpUtil.startThisServer();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		/*
		 * while(!shutdownRequested)
		 * {
		 * try{Thread.sleep(60000);}
		 * catch(InterruptedException e)
		 * {
		 * e.printStackTrace();
		 * continue;
		 * }
		 * }
		 * System.exit(0);
		 */
		// try { Thread.sleep(60000); }//Puts Thread asleep for one minute to wait for commands via
		// REST-interface
		// catch(InterruptedException e) { e.printStackTrace(); }

	}
}