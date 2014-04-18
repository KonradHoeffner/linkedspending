package org.aksw.linkedspending;

/** Pre-version of planned scheduler, which can (directly) control the JsonDownloader (start/stop/pause, all/specified datasets).*/
public class Scheduler
{

    /** Starts JsonDownloader */
    protected static void runDownloader()
    {
        Thread jDl = new Thread(new JsonDownloader());
        jDl.start();
        /*try
        {
            JsonDownloader.downloadAll();

            JsonDownloader.puzzleTogether();
        } catch (Exception e)
        {
            //TODO: Error Handling
        }*/
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
        //long startTime = System.currentTimeMillis();
       // System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
        //try{LogManager.getLogManager().readConfiguration();log.setLevel(Level.FINER);} catch ( Exception e ) { e.printStackTrace();}
        runDownloader();
        stopDownloader();
        //log.info("Processing time: "+(System.currentTimeMillis()-startTime)/1000+" seconds. Maximum memory usage of "+memoryBenchmark.updateAndGetMaxMemoryBytes()/1000000+" MB.");
        //System.exit(0); // circumvent non-close bug of ObjectMapper.readTree
    }
}
