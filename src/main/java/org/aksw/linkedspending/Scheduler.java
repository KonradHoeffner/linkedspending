package org.aksw.linkedspending;


/** Pre-version of planned scheduler, which can (directly) control the JsonDownloader (start/stop/pause, all/specified datasets).*/
public class Scheduler
{
    //private static Thread dlThread;
    //private static Thread convThread;

    /** Starts complete download */
    protected static void runDownloader()
    {
        JsonDownloader j = new JsonDownloader();
        j.setStopRequested(false);
        j.setPauseRequested(false);
        j.setCompleteRun(true);
        //Thread jDl = new Thread(new JsonDownloader());
        Thread jDl = new Thread(j);
        jDl.start();
    }

    /** Stops JsonDownloader. Already started downloads of datasets will be finished, but no new downloads will be started. */
    protected static void stopDownloader() {JsonDownloader.setStopRequested(true);}

    /** Pauses JsonDownloader */
    protected static void pauseDownloader() {JsonDownloader.setPauseRequested(true);}

    /** Resumes downloading process */
    protected static void resumeDownload() { JsonDownloader.setPauseRequested(false); }

    /** Starts downloading a specified dataset */
    protected static void downloadDataset(String datasetName)
    {
        JsonDownloader j = new JsonDownloader();
        j.setCompleteRun(false);
        j.setToBeDownloaded(datasetName);
        Thread jThr = new Thread(j);
        jThr.start();
    }

    /** Starts converting of all new Datasets */
    protected static void runConverter()
    {
        Thread convThr = new Thread(new Converter());
        Converter.setPauseRequested(false);
        Converter.setStopRequested(false);
        convThr.start();
    }

    /** Stops the converting process */
    protected static void stopConverter() { Converter.setStopRequested(true); }

    /** Pauses converting process */
    protected static void pauseConverter() { Converter.setPauseRequested(true); }

    /** Resumes converting process */
    protected static void resumeConverter() { Converter.setPauseRequested(false); }

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
