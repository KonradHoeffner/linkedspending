package org.aksw.linkedspending;

import org.aksw.linkedspending.converter.Converter;
import org.aksw.linkedspending.downloader.JsonDownloader;

/** Pre-version of planned scheduler, which can (directly) control the JsonDownloader (start/stop/pause, all/specified datasets).*/
//@Path("control")
public class Scheduler
{
    //private static Thread dlThread;
    //private static Thread convThread;
    //todo: delete those three methods later
    /*@GET
    @Path("{param}")
    public Response getMsg(@PathParam("param") String msg) {
        String output = "Jersey say : " + msg;

        return Response.status(200).entity(output).build();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String sayHelloInPlainText() {
        return "Hello world!";
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public String sayHelloInHtml() {
        return "<html> " + "<title>" + "Hello world!" + "</title>"
                + "<body><h1>" + "Hello world!" + "</body></h1>" + "</html> ";
    }
    */

    /** Starts complete download */
    //@GET
    //@Path("downloadcomplete")
    public static String runDownloader()
    {
        JsonDownloader j = new JsonDownloader();
        j.setStopRequested(false);
        j.setPauseRequested(false);
        j.setCompleteRun(true);
        //Thread jDl = new Thread(new JsonDownloader());
        Thread jDl = new Thread(j);
        jDl.start();
        return "Started complete download";
    }

    /** Stops JsonDownloader. Already started downloads of datasets will be finished, but no new downloads will be started. */
    //@GET
    //@Path("stopdownload")
    public static String stopDownloader()
    {
        JsonDownloader.setStopRequested(true);
        return "Stopped downloading";
    }

    /** Pauses JsonDownloader */
    //@GET
    //@Path("pausedownload")
    public static String pauseDownloader()
    {
        JsonDownloader.setPauseRequested(true);
        return "Paused Downloader";
    }

    /** Resumes downloading process */
    //@GET
    //@Path("resumedownload")
    public static String resumeDownload()
    {
        JsonDownloader.setPauseRequested(false);
        return "Resumed Downloader";
    }

    /** Starts downloading a specified dataset */
    //@Path("downlaodspecific/{param}")
    protected static void downloadDataset(String datasetName/*, @PathParam("param") String datasetName */)
    {
        JsonDownloader j = new JsonDownloader();
        j.setCompleteRun(false);
        j.setToBeDownloaded(datasetName);
        Thread jThr = new Thread(j);
        jThr.start();
    }

    /** Starts converting of all new Datasets */
    //@GET @Produces(MediaType.TEXT_PLAIN)
    //@Path("convertcomplete")
    public static String runConverter()
    {
        Thread convThr = new Thread(new Converter());
        Converter.setPauseRequested(false);
        Converter.setStopRequested(false);
        convThr.start();
        return "Started Converter.";
    }

    /** Stops the converting process */
    //@GET
    //@Path("stopconvert")
    public static String stopConverter()
    {
        Converter.setStopRequested(true);
        return "Stopped Converter.";
    }

    /** Pauses converting process */
    //@GET
    //@Path("pauseconvert")
    public static String pauseConverter() {
        Converter.setPauseRequested(true);
        return "Paused Converter.";
    }


    /** Resumes converting process */
    //@GET
    //@Path("resumeconvert")
    public static String resumeConverter() {
        Converter.setPauseRequested(false);
        return "Resumed Converter";
    }

    public static void main(String[] args)
    {
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

        runConverter();
        //pauseConverter();
        //resumeConverter();
        //stopConverter();

    }
}
