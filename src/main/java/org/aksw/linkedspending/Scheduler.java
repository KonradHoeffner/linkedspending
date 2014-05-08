package org.aksw.linkedspending;

import org.aksw.linkedspending.converter.Converter;
import org.aksw.linkedspending.downloader.JsonDownloader;
import org.aksw.linkedspending.tools.GrizzlyHttpUtil;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

/** Pre-version of planned scheduler, which can (directly) control the JsonDownloader (start/stop/pause, all/specified datasets).*/
@Path("control")
public class Scheduler
{
    //public static final String BASE_URI = "http://localhost:8080/myapp/";
    private static final URI baseURI = UriBuilder.fromUri("http://localhost/").port(9998).build();
    //private static final HttpServer server;

    private static Thread downloaderThread;
    private static Thread converterThread;
    private static JsonDownloader downloader = new JsonDownloader();
    private static Converter converter = new Converter();

    /** Starts complete download */
    @GET
    @Path("downloadcomplete")
    public static String runDownloader()
    {
        //JsonDownloader j = new JsonDownloader();
        //j.setStopRequested(false);
        //j.setPauseRequested(false);
        //j.setCompleteRun(true);

        downloader.setStopRequested(false);
        downloader.setPauseRequested(false);
        downloader.setCompleteRun(true);
        //Thread jDl = new Thread(new JsonDownloader());
        /*Thread jDl = new Thread(j);
        jDl.start();*/
        downloaderThread = new Thread(downloader);
        downloaderThread.start();
        return "Started complete download";
    }

    /** Stops JsonDownloader. Already started downloads won't be finished, use it carefully! */
    @GET
    @Path("stopdownload")
    public static String stopDownloader()
    {

        /*downloader.setStopRequested(true);
        while(!downloader.getDownloadStopped())
        {
            try{ Thread.sleep(200); }
            catch(InterruptedException e) { }
        }*/
        downloaderThread.interrupt();
        return "Stopped downloading";
    }

    /** Pauses JsonDownloader */
    @GET
    @Path("pausedownload")
    public static String pauseDownloader()
    {
        downloader.setPauseRequested(true);
        return "Paused Downloader";
    }

    /** Resumes downloading process */
    @GET
    @Path("resumedownload")
    public static String resumeDownload()
    {
        downloader.setPauseRequested(false);
        return "Resumed Downloader";
    }

    /** Starts downloading a specified dataset */
    @Path("downlaodspecific/{param}")
    public static String downloadDataset(/*String datasetName,*/ @PathParam("param") String datasetName )
    {
        /*JsonDownloader j = new JsonDownloader();
        j.setCompleteRun(false);
        j.setToBeDownloaded(datasetName);
        Thread jThr = new Thread(j);
        jThr.start();*/
        downloader.setCompleteRun(false);
        downloader.setToBeDownloaded(datasetName);
        downloaderThread = new Thread(downloader);
        downloaderThread.start();
        return "Started downloading dataset " + datasetName;
    }

    /** Starts converting of all new Datasets */
    @GET @Produces(MediaType.TEXT_PLAIN)
    @Path("convertcomplete")      //localhost:8080/openspending2rdfbla.war/control/convertcomplete
    public static String runConverter()
    {
        /*Thread convThr = new Thread(new Converter());
        Converter.setPauseRequested(false);
        Converter.setStopRequested(false);
        convThr.start();*/

        converter.setPauseRequested(false);
        converter.setStopRequested(false);
        converterThread = new Thread(converter);
        converterThread.start();
        return "Started Converter.";
    }

    /** Stops the converting process */
    @GET
    @Path("stopconvert")
    public static String stopConverter()
    {
        //Converter.setStopRequested(true);
        converterThread.interrupt();
        return "Stopped Converter.";
    }

    /** Pauses converting process */
    @GET
    @Path("pauseconvert")
    public static String pauseConverter() {
        Converter.setPauseRequested(true);
        return "Paused Converter.";
    }

    /** Resumes converting process */
    @GET
    @Path("resumeconvert")
    public static String resumeConverter() {
        Converter.setPauseRequested(false);
        return "Resumed Converter";
    }

    @GET
    @Path("shutdown")
    public static String shutdown()
    {
        if(downloaderThread != null) stopDownloader();
        if(converterThread != null) stopConverter();
        GrizzlyHttpUtil.shutdownGrizzly();
        return "Service shutted down.";
    }

    public static void main(String[] args)
    {
        try{ GrizzlyHttpUtil.startServer(); }
        catch (Exception e) {e.printStackTrace();}
        try { Thread.sleep(60000); }    //Puts Thread asleep for one minute to wait for commands via REST-interface
        catch(InterruptedException e) {e.printStackTrace();}

        //downloadDataset("berlin_de");
        //runDownloader();
        //pauseDownloader();
        //resumeDownload();
        //stopDownloader();

        /*while(!JsonDownloader.finished) {}
        for(EventNotification eN : JsonDownloader.getEventContainer().getEventNotifications())
        {
            System.out.println(eN.getEventCode(true));
        }*/

        //runConverter();
        //pauseConverter();
        //resumeConverter();
        //stopConverter();

    }
}
