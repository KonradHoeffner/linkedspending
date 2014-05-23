package org.aksw.linkedspending;

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

    @Before
    public void setUp() throws Exception
    {
        URI uri = UriBuilder.fromUri("http://localhost/").port(10010).build();
        // start the server
        //server = GrizzlyHttpUtil.startServer();
        ResourceConfig resCon = new ResourceConfig().packages("org.aksw.linkedspending");
        server = GrizzlyHttpServerFactory.createHttpServer(uri, resCon);

        // create the client
        Client c = ClientBuilder.newClient();
        target = c.target(Scheduler.getBaseURI());
    }

    @After
    public void tearDown() throws Exception
    {
        //server.stop();
        server.shutdown();
    }

    @Test
    public void testRunDownloader()
    {
        //Tests REST-functionality
        String responseMsg = target.path("control/downloadcomplete").request().get(String.class);
        Assert.assertEquals("Started complete download", responseMsg);

        //checks if files are being downloaded
        try{Thread.sleep(5000);}
        catch(InterruptedException e) {}

        File f = new File("/json");
        long totalSpace = f.getTotalSpace();

        try{Thread.sleep(20000);}
        catch(InterruptedException e) {}
        long totalSpace2 = f.getTotalSpace();

        Assert.assertTrue(totalSpace < totalSpace2);
    }

    @Test
    public void testStopDownloader()
    {
        String responseMsg = target.path("control/stopdownload").request().get(String.class);
        Assert.assertEquals("Stopped downloading", responseMsg);
        try{Thread.sleep(30000);}
        catch(InterruptedException e) {}

        File f = new File("/json");
        long before = f.getTotalSpace();

        try{Thread.sleep(10000);}
        catch(InterruptedException e) {}

        long after = f.getTotalSpace();
        Assert.assertTrue(after == before);
    }

    @Test
    public void testPauseDownloader()
    {
        String responseMsg = target.path("control/pausedownload").request().get(String.class);
        Assert.assertEquals("Paused Downloader", responseMsg);

        File f = new File("/json");
        long totalSpace = f.getTotalSpace();
        try{Thread.sleep(20000);}
        catch(InterruptedException e) {}
        long totalSpace2 = f.getTotalSpace();

        Assert.assertTrue(totalSpace == totalSpace2);
    }

    @Test
    public void testResumeDownloader()
    {
        String responseMsg = target.path("control/resumedownload").request().get(String.class);
        Assert.assertEquals("Resumed Downloader", responseMsg);

        File f = new File("/json");
        long totalSpace = f.getTotalSpace();
        try{Thread.sleep(20000);}
        catch(InterruptedException e) {}
        long totalSpace2 = f.getTotalSpace();

        Assert.assertTrue(totalSpace < totalSpace2);
    }

    @Test
    public void testDownloadDataset()
    {
        String responseMsg = target.path("control/downloadspecific/berlin_de").request().get(String.class);
        Assert.assertEquals("Started downloading dataset " + "berlin_de", responseMsg);

        try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        File f = new File("json/berlin_de");
        Assert.assertTrue(f.isFile() && (f.getTotalSpace() > 0));      //does berlin_de exist and is not empty?
    }

    @Test
    public void testConvertComplete()
    {
        String responseMsg = target.path("control/convert").request().get(String.class);
        Assert.assertEquals("Started Converter", responseMsg);

        File f = new File("/output");
        long start = f.getTotalSpace();
        try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        long later = f.getTotalSpace();     //did something change in output directory?
        Assert.assertTrue(start != later);
    }

    @Test
    public void testStopConvert()
    {
        String responseMsg = target.path("control/stopconvert").request().get(String.class);
        Assert.assertEquals("Stopped Converter", responseMsg);

        boolean test = Scheduler.getConverter().getEventContainer().checkForEvent(EventNotification.EventType.stoppedConverter, EventNotification.EventSource.Converter);
        Assert.assertTrue(test == true);
    }

    @Test
    public void testPauseConverter()
    {
        String responseMsg = target.path("control/pauseconvert").request().get(String.class);
        Assert.assertEquals("Paused Converter", responseMsg);

        File f = new File("/output");
        long before = f.getTotalSpace();
        try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        long after = f.getTotalSpace();

        Assert.assertTrue(after == before);
    }

    @Test
    public void testResumeConverter()
    {
        File f = new File("/output");
        long before = f.getTotalSpace();

        String responseMsg = target.path("control/resumeconvert").request().get(String.class);
        Assert.assertEquals("Resumed Converter", responseMsg);

        try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        long after = f.getTotalSpace();

        Assert.assertTrue(after != before);
    }

    @Test
    public void testShutdown()
    {
        String responseMsg = target.path("control/shutdown").request().get(String.class);
        Assert.assertEquals("Service shutted down.", responseMsg);
    }
}
