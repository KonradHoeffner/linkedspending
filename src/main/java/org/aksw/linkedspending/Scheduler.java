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
    public static final URI baseURI = UriBuilder.fromUri("http://localhost/").port(9998).build();
    //private static final HttpServer server;

    public static Thread downloaderThread;
    public static Thread converterThread;

    /** Starts complete download */
    @GET
    @Path("downloadcomplete")
    public static String runDownloader()
    {
        JsonDownloader j = new JsonDownloader();
        j.setStopRequested(false);
        j.setPauseRequested(false);
        j.setCompleteRun(true);
        //Thread jDl = new Thread(new JsonDownloader());
        /*Thread jDl = new Thread(j);
        jDl.start();*/
        downloaderThread = new Thread(j);
        downloaderThread.start();
        return "Started complete download";
    }

    /** Stops JsonDownloader. Already started downloads of datasets will be finished, but no new downloads will be started. */
    @GET
    @Path("stopdownload")
    public static String stopDownloader()
    {
        JsonDownloader.setStopRequested(true);
        downloaderThread.interrupt();
        return "Stopped downloading";
    }

    /** Pauses JsonDownloader */
    @GET
    @Path("pausedownload")
    public static String pauseDownloader()
    {
        JsonDownloader.setPauseRequested(true);
        return "Paused Downloader";
    }

    /** Resumes downloading process */
    @GET
    @Path("resumedownload")
    public static String resumeDownload()
    {
        JsonDownloader.setPauseRequested(false);
        return "Resumed Downloader";
    }

    /** Starts downloading a specified dataset */
    @Path("downlaodspecific/{param}")
    public static String downloadDataset(/*String datasetName,*/ @PathParam("param") String datasetName )
    {
        JsonDownloader j = new JsonDownloader();
        j.setCompleteRun(false);
        j.setToBeDownloaded(datasetName);
        Thread jThr = new Thread(j);
        jThr.start();
        return "Started downloading dataset " + datasetName;
    }

    /** Starts converting of all new Datasets */
    @GET @Produces(MediaType.TEXT_PLAIN)
    @Path("convertcomplete")      //localhost:8080/openspending2rdfbla.war/control/convertcomplete
    public static String runConverter()
    {
        Thread convThr = new Thread(new Converter());
        Converter.setPauseRequested(false);
        Converter.setStopRequested(false);
        convThr.start();
        return "Started Converter.";
    }

    /** Stops the converting process */
    @GET
    @Path("stopconvert")
    public static String stopConverter()
    {
        Converter.setStopRequested(true);
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

   /* private static void startGrizzly()  throws Exception
    {

        final String baseUri = "http://localhost:9998/";
        final Map<String, String> initParams =
                new HashMap<String, String>();

        initParams.put("com.sun.jersey.config.property.packages",
                "com.sun.jersey.samples.helloworld.resources");

        System.out.println("Starting grizzly...");
        SelectorThread threadSelector =
                GrizzlyWebContainerFactory.create(baseUri, initParams);
        System.out.println(String.format(
                "Jersey app started with WADL available at %sapplication.wadl\n” +
                “Try out %shelloworld\nHit enter to stop it...", baseUri, baseUri));
        System.in.read();
        threadSelector.stopEndpoint();
        System.exit(0);
    }*/
    /*
    public static HttpServer startServer() {
        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        //final ResourceConfig rc = new ResourceConfig().packages("org.aksw.linkedspending");
        ResourceConfig resCon = new ResourceConfig(Scheduler.class);

        //todo: fix error -> NoClassDefFoundError: javax/ws/rs/core/Configurable
        System.out.println("ResourceConfig fine...");
        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        //return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI));
        return GrizzlyHttpServerFactory.createHttpServer(baseURI, resCon);
    }

    public static void startGrizzly() throws IOException {
        final HttpServer server = startServer();
        System.out.println("startServer fine...");
        System.out.println(String.format("Jersey app started with WADL available at "
                + "%sapplication.wadl\nHit enter to stop it...", baseURI));

        System.in.read();
        server.stop();
    }*/

    @GET
    @Path("shutdown")
    public static String shutdown()
    {
        stopDownloader();
        stopConverter();
        GrizzlyHttpUtil.shutdownGrizzly();
        return "Shutting down service.";
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
