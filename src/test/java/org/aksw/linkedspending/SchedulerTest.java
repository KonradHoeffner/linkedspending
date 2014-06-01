package org.aksw.linkedspending;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.hamcrest.SelfDescribing;
import org.aksw.linkedspending.tools.EventNotification;
import org.aksw.linkedspending.tools.GrizzlyHttpUtil;
import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.net.URI;

/** Tests if Scheduler reacts properly to incoming commands via REST-interface */
public class SchedulerTest
{
    private HttpServer server;
    private WebTarget target;
    private Client c;

    @Before
    public void setUp() throws Exception
    {
        //URI uri = UriBuilder.fromUri("http://localhost/").port(10010).build();
        // start the server
        server = GrizzlyHttpUtil.startServer(10011);

        //ResourceConfig resCon = new ResourceConfig().packages("org.aksw.linkedspending");
        //server = GrizzlyHttpServerFactory.createHttpServer(uri, resCon);

        // create the client
        //c = ClientBuilder.newClient();
        /*todo: there seems to be a bug with creating a client (throws nullpointerexception) at the moment, try fix this later. */
        //todo: edit: might have been caused by doubly starting http server, which is fixed now. Check tests again.
        ClientConfig clientConfig = new ClientConfig();
        c = ClientBuilder.newClient();
        target = c.target(UriBuilder.fromUri("http://localhost/").port(10010).build());
    }

    @After
    public void tearDown() throws Exception
    {
        //server.stop();
        server.shutdown();
    }

    /*
    This funktionality is already better tested in JsonDownloaderIT.downloaderTest().
    Also what is long totalSpace = f.getTotalSpace(); supposed to do?
    Please rethink or delete this.
    @Test
    public void testRunDownloader()
    {
        //Tests REST-functionality
        //String responseMsg = target.path("http://localhost:10010/control/downloadcomplete").request().get(String.class);
        //Assert.assertEquals("Started complete download", responseMsg);
        Scheduler.runManually();
        Scheduler.runDownloader();
        //checks if files are being downloaded
        try{Thread.sleep(5000);}
        catch(InterruptedException e) {}

        /*File f = new File("/json");
        long totalSpace = f.getTotalSpace();

        try{Thread.sleep(20000);}
        catch(InterruptedException e) {}
        long totalSpace2 = f.getTotalSpace();

        Assert.assertTrue(totalSpace < totalSpace2);*/
        boolean b = Scheduler.getDownloader().getEventContainer().checkForEvent(EventNotification.EventType.startedDownloadingComplete, EventNotification.EventSource.Downloader);
        Assert.assertTrue(b);
    }
    */

    @Test
    public void testStopDownloader()
    {
        //String responseMsg = target.path("control/stopdownload").request().get(String.class);
        //Assert.assertEquals("Stopped downloading", responseMsg);
        Scheduler.runManually();
        Scheduler.stopDownloader();
        try{Thread.sleep(30000);}
        catch(InterruptedException e) {}

        /*File f = new File("/json");
        long before = f.getTotalSpace();

        try{Thread.sleep(10000);}
        catch(InterruptedException e) {}

        long after = f.getTotalSpace();
        Assert.assertTrue(after == before);*/
        //if the Downloader was successfully stopped, several DownloadCallables will add finishedDownloadingDataset (success == false) events after a while.
        boolean b = Scheduler.getDownloader().getEventContainer().checkForEvent(EventNotification.EventType.downloadStopped, EventNotification.EventSource.Downloader);
        Assert.assertTrue(b);
    }

    @Test
    public void testPauseDownloader()
    {
        //String responseMsg = target.path("control/pausedownload").request().get(String.class);
        //Assert.assertEquals("Paused Downloader", responseMsg);
        Scheduler.runManually();
        Scheduler.runDownloader();
        Scheduler.pauseDownloader();

        //Did the size of /json folder change after downloader has been paused?
        /*File f = new File("/json");
        long totalSpace = f.getTotalSpace();
        try{Thread.sleep(5000);}
        catch(InterruptedException e) {}
        long totalSpace2 = f.getTotalSpace();

        Assert.assertTrue(totalSpace == totalSpace2);*/
        boolean b = Scheduler.getDownloader().getEventContainer().checkForEvent(EventNotification.EventType.downloadPaused, EventNotification.EventSource.Downloader);
        Assert.assertTrue(b);
    }

    @Test
    public void testResumeDownloader()
    {
        //String responseMsg = target.path("control/resumedownload").request().get(String.class);
        //Assert.assertEquals("Resumed Downloader", responseMsg);
        Scheduler.runManually();
        Scheduler.runDownloader();
        Scheduler.pauseDownloader();
        Scheduler.resumeDownload();

        /*File f = new File("/json");
        long totalSpace = f.getTotalSpace();
        try{Thread.sleep(20000);}
        catch(InterruptedException e) {}
        long totalSpace2 = f.getTotalSpace();

        Assert.assertTrue(totalSpace < totalSpace2);*/

        boolean b = Scheduler.getDownloader().getEventContainer().checkForEvent(EventNotification.EventType.downloadResumed, EventNotification.EventSource.Downloader);
        Assert.assertTrue(b);
    }

    @Test
    public void testDownloadDataset()
    {
        //String responseMsg = target.path("control/downloadspecific/berlin_de").request().get(String.class);
        //Assert.assertEquals("Started downloading dataset " + "berlin_de", responseMsg);
        Scheduler.runManually();
        Scheduler.downloadDataset("berlin_de");

        /*try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        File f = new File("json/berlin_de");
        Assert.assertTrue(f.isFile() && (f.getTotalSpace() > 0));      //does berlin_de exist and is not empty?
        */
        boolean b = Scheduler.getDownloader().getEventContainer().checkForEvent(EventNotification.EventType.startedDownloadingSingle, EventNotification.EventSource.Downloader);
        Assert.assertTrue(b);
    }

    @Test
    public void testConvertComplete()
    {
        //String responseMsg = target.path("control/convert").request().get(String.class);
        //Assert.assertEquals("Started Converter", responseMsg);
        Scheduler.runManually();
        Scheduler.runConverter();

        try {Thread.sleep(6000);}
        catch(InterruptedException e) {}
        /*
        File f = new File("/output");
        long start = f.getTotalSpace();
        try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        long later = f.getTotalSpace();     //did something change in output directory?
        Assert.assertTrue(start != later); */

        boolean b = Scheduler.getConverter().getEventContainer().checkForEvent(EventNotification.EventType.startedConvertingComplete, EventNotification.EventSource.Converter);
        Assert.assertTrue(b);
    }

    @Test
    public void testStopConvert()
    {
        //String responseMsg = target.path("control/stopconvert").request().get(String.class);
        //Assert.assertEquals("Stopped Converter", responseMsg);
        Scheduler.runManually();
        Scheduler.runConverter();
        Scheduler.stopConverter();

        try {Thread.sleep(8000);}
        catch(InterruptedException e) {}

        boolean b = Scheduler.getConverter().getEventContainer().checkForEvent(EventNotification.EventType.stoppedConverter, EventNotification.EventSource.Converter);
        Assert.assertTrue(b);
    }

    @Test
    public void testPauseConverter()
    {
        //String responseMsg = target.path("control/pauseconvert").request().get(String.class);
        //Assert.assertEquals("Paused Converter", responseMsg);
        Scheduler.runManually();
        Scheduler.runConverter();
        Scheduler.pauseConverter();

        try {Thread.sleep(8000);}
        catch(InterruptedException e) {}
        /*
        File f = new File("/output");
        long before = f.getTotalSpace();
        try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        long after = f.getTotalSpace();

        Assert.assertTrue(after == before);*/

        boolean b = Scheduler.getConverter().getEventContainer().checkForEvent(EventNotification.EventType.pausedConverter, EventNotification.EventSource.Converter);
        Assert.assertTrue(b);
    }

    @Test
    public void testResumeConverter()
    {
        //File f = new File("/output");
        //long before = f.getTotalSpace();

        Scheduler.runManually();
        Scheduler.runConverter();
        Scheduler.pauseConverter();
        Scheduler.resumeConverter();
        //String responseMsg = target.path("control/resumeconvert").request().get(String.class);
        //Assert.assertEquals("Resumed Converter", responseMsg);

        try {Thread.sleep(8000);}
        catch(InterruptedException e) {}
        /*
        try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        long after = f.getTotalSpace();

        Assert.assertTrue(after != before);*/

        boolean b = Scheduler.getConverter().getEventContainer().checkForEvent(EventNotification.EventType.resumedConverter, EventNotification.EventSource.Converter);
        Assert.assertTrue(b);
    }

    @Test
    public void testShutdown()
    {
        //String responseMsg = target.path("control/shutdown").request().get(String.class);
        //Assert.assertEquals("Service shutted down.", responseMsg);
    }
}
