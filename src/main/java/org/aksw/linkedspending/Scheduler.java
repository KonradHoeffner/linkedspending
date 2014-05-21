package org.aksw.linkedspending;

import org.aksw.linkedspending.converter.Converter;
import org.aksw.linkedspending.downloader.JsonDownloader;
import org.aksw.linkedspending.tools.ConverterSleeper;
import org.aksw.linkedspending.tools.GrizzlyHttpUtil;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

/** Pre-version of planned scheduler, which can (directly) control the JsonDownloader (start/stop/pause, all/specified datasets).*/
@Path("control")
public class Scheduler
{
    private static final URI baseURI = UriBuilder.fromUri("http://localhost/").port(9998).build();

    private static Thread scheduleTimeThread;
    private static Thread downloaderThread;
    private static Thread converterThread;
    private static JsonDownloader downloader = new JsonDownloader();
    private static Converter converter = new Converter();
    private static ScheduleTimeHandler scheduleTimeHandler;

    public static URI getBaseURI() {return baseURI;}

    protected static Thread getDownloaderThread() {return downloaderThread;}
    protected static Thread getConverterThread() {return converterThread;}
    public static JsonDownloader getDownloader() {return downloader;}
    public static Converter getConverter() {return converter;}

    private static boolean manualMode = false;

    private static void shutdownScheduleTimeHandler()
    {
        scheduleTimeHandler.setShutdownRequested(true);
    }

    /**
     * Sets a new start date for scheduled runs (e.g. setstartdate/7/20 sets start time to 20 o'clock at Sunday)
     * @param startDay The starting day (1 = Monday, ... , 7 = Sunday)
     * @param startHour The starting hour
     * @param repeat The ratio to repeat in weeks (e.g. 2 means one run every two weeks)
     */
    @Path("setstartdate/{day}/{hour}/{repeat}")
    public static String setScheduleTime(@PathParam("day") String startDay, @PathParam("hour") String startHour, @PathParam("repeat") String repeat)
    {
        scheduleTimeHandler.setShutdownRequested(true);

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
    @GET
    @Path("runmanually")
    public static String runManually()
    {
        manualMode = true;
        shutdownScheduleTimeHandler();
        return "Manual running has been started.";
    }

    /**
     * Resets the programm to run in automatic mode after it has been run manually.
     * Has no effect if it hasn't been run manually yet.
     */
    @GET
    @Path("runautomatically")
    public static String runAutomatically()
    {
        if(!manualMode) return "Programm already running automatically.";
        else
        {
            manualMode = false;
            scheduleTimeHandler.setShutdownRequested(false);
            scheduleTimeThread = new Thread(scheduleTimeHandler);
            scheduleTimeThread.start();
            return "Program now is in automatic mode.";
        }
    }

    /** Starts complete download.
     * To be used for out-of-schedule runs only. */
    @GET
    @Path("downloadcomplete")
    public static String runDownloader()
    {
        if (manualMode)
        {
            downloader.setStopRequested(false);
            downloader.setPauseRequested(false);
            downloader.setCompleteRun(true);
            downloaderThread = new Thread(downloader);
            downloaderThread.start();
            return "Started complete download";
        }
        else return "Error: Program not in manual mode!";
    }

    /** Stops JsonDownloader. Already started downloads won't be finished, use it carefully! */
    @GET
    @Path("stopdownload")
    public static String stopDownloader()
    {
        if(manualMode)
        {
            downloader.setStopRequested(true);
            return "Stopped downloading";
        }
        else return "Error: Program not in manual mode!";
    }

    /** Pauses JsonDownloader */
    @GET
    @Path("pausedownload")
    public static String pauseDownloader()
    {
        if (manualMode)
        {
            downloader.setPauseRequested(true);
            return "Paused Downloader";
        }
        else return "Error: Program not in manual mode!";
    }
    /** Resumes downloading process */
    @GET
    @Path("resumedownload")
    public static String resumeDownload()
    {
        if(manualMode)
        {
        downloader.setPauseRequested(false);
        return "Resumed Downloader";
        }
        else return "Error: Program not in manual mode!";
    }

    /** Starts downloading a specified dataset */
    @Path("downlaodspecific/{param}")
    public static String downloadDataset( @PathParam("param") String datasetName )
    {
        if(manualMode)
        {
            downloader.setCompleteRun(false);
            downloader.setToBeDownloaded(datasetName);
            downloaderThread = new Thread(downloader);
            downloaderThread.start();
            return "Started downloading dataset " + datasetName;
        }
        else return "Error: Program not in manual mode!";
    }

    /** Starts converting of all new Datasets.
     * To be used for out-of-schedule runs only. */
    @GET
    @Path("convertcomplete")
    public static String runConverter()
    {
        if(manualMode)
        {
        converter.setPauseRequested(false);
        converter.setStopRequested(false);
        converterThread = new Thread(converter);
        converterThread.start();
        return "Started Converter.";
        }
        else return "Error: Program not in manual mode!";
    }

    /** Stops the converting process */
    @GET
    @Path("stopconvert")
    public static String stopConverter()
    {
        if(manualMode)
        {
            //converterThread.interrupt();
            converter.setStopRequested(true);
            return "Stopped Converter.";
        }
        else return "Error: Program not in manual mode!";
    }

    /** Pauses converting process */
    @GET
    @Path("pauseconvert")
    public static String pauseConverter()
    {
        if(manualMode)
        {
            Converter.setPauseRequested(true);
            ConverterSleeper cS = new ConverterSleeper();
            Thread cSThread = new Thread(cS);
            cSThread.start();
            return "Paused Converter.";
        }
        else return "Error: Program not in manual mode!";
    }

    /** Resumes converting process */
    @GET
    @Path("resumeconvert")
    public static String resumeConverter()
    {
        if(manualMode)
        {
            Converter.setPauseRequested(false);
            return "Resumed Converter";
        }
        else return "Error: Program not in manual mode!";
    }

    /** Completly shutdowns downloader, converter (if running) and scheduler */
    @GET
    @Path("shutdown")
    public static String shutdown()
    {
        if(downloaderThread != null) stopDownloader();
        if(converterThread != null) stopConverter();
        GrizzlyHttpUtil.shutdownGrizzly();
        return "Service shut down.";
    }

    /** Runs a complete download after a specified period of time and starts converting afterwards.
     * To be used for out-of-schedule runs only.
     * @Param timeTillStart the specified point of time
     * @Param unit the unit of time measurement (d for days, min for minutes)*/
    @GET
    @Path("schedule/{time}/{unit}")
    public static String scheduleCompleteRun(@PathParam("time") int timeTillStart, @PathParam("unit") String unit)
    {
        if(manualMode)
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

    /** Helping method for String scheduleCompleteRun(). Waits specified time and runs downloader and converter.
     * Converter is run after download has finished. */
    private static void scheduleCompleteRun(long timeInMs)
    {
        try { Thread.sleep(timeInMs); }
        catch (InterruptedException e) { }

        runDownloader();

        converter.setPauseRequested(false);
        converter.setStopRequested(false);
        converterThread = new Thread(converter);

        ConverterSleeper cS = new ConverterSleeper();
        Thread cSThread = new Thread(cS);
        converterThread.start();
        cSThread.start();
    }

    public static void main(String[] args)
    {
        //todo: @ScheduleTimeHandler: can environmentVariables be changed while the programm is running?
        //todo: If yes, should be considered in ScheduleTimeHandler.
        //ScheduleTimeHandler sth = new ScheduleTimeHandler();
        scheduleTimeHandler = new ScheduleTimeHandler();
        scheduleTimeThread = new Thread(scheduleTimeHandler);
        scheduleTimeThread.start();


        try {GrizzlyHttpUtil.startServer();}
        catch (Exception e) { e.printStackTrace();}
        try { Thread.sleep(60000); }//Puts Thread asleep for one minute to wait for commands via REST-interface
        catch(InterruptedException e) { e.printStackTrace(); }

    }
}