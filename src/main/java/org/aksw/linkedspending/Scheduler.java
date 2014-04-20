package org.aksw.linkedspending;

/** Pre-version of planned scheduler, which can (directly) control the JsonDownloader (start/stop/pause, all/specified datasets).*/
public class Scheduler
{

    /** Starts JsonDownloader in new thread */
    protected static void runDownloader()
    {
        Thread jDl = new Thread(new JsonDownloader());
        jDl.start();
    }

    /** Stops JsonDownloader. Already started downloads of datasets will be finished, but no new downloads will be started. */
    protected static void stopDownloader() {JsonDownloader.setCurrentlyRunning(false);}

    /** Resumes downloading process */
    protected static void resumeDownload()
    {
        JsonDownloader.setCurrentlyRunning(true);
        runDownloader();        //improve performance: does everything up to point from where to resume as well
    }

    public static void main(String[] args)
    {
       runDownloader();
        //stopDownloader();
    }
}
