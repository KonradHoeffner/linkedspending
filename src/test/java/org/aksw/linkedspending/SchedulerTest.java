package org.aksw.linkedspending;


import org.aksw.linkedspending.tools.EventNotification;
import org.aksw.linkedspending.tools.GrizzlyHttpUtil;
import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.io.File;

import static org.junit.Assert.*;

/** Tests if Scheduler reacts properly to incoming commands via REST-interface */
public class SchedulerTest
{
    private HttpServer server;
    private WebTarget target;

    @Before
    public void setUp() throws Exception
    {
        // start the server
        server = GrizzlyHttpUtil.startServer();
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
        assertEquals("Started complete download", responseMsg);

        //checks if files are being downloaded
        try{Thread.sleep(5000);}
        catch(InterruptedException e) {}

        File f = new File("/json");
        long totalSpace = f.getTotalSpace();

        try{Thread.sleep(20000);}
        catch(InterruptedException e) {}
        long totalSpace2 = f.getTotalSpace();

        assertTrue(totalSpace < totalSpace2);
    }

    @Test
    public void testStopDownloader()
    {
        String responseMsg = target.path("control/stopdownload").request().get(String.class);
        assertEquals("Stopped downloading", responseMsg);
        try{Thread.sleep(30000);}
        catch(InterruptedException e) {}

        File f = new File("/json");
        long before = f.getTotalSpace();

        try{Thread.sleep(10000);}
        catch(InterruptedException e) {}

        long after = f.getTotalSpace();
        assertTrue(after==before);
    }

    @Test
    public void testPauseDownloader()
    {
        String responseMsg = target.path("control/pausedownload").request().get(String.class);
        assertEquals("Paused Downloader", responseMsg);

        File f = new File("/json");
        long totalSpace = f.getTotalSpace();
        try{Thread.sleep(20000);}
        catch(InterruptedException e) {}
        long totalSpace2 = f.getTotalSpace();

        assertTrue(totalSpace == totalSpace2);
    }

    @Test
    public void testResumeDownloader()
    {
        String responseMsg = target.path("control/resumedownload").request().get(String.class);
        assertEquals("Resumed Downloader", responseMsg);

        File f = new File("/json");
        long totalSpace = f.getTotalSpace();
        try{Thread.sleep(20000);}
        catch(InterruptedException e) {}
        long totalSpace2 = f.getTotalSpace();

        assertTrue(totalSpace < totalSpace2);
    }

    @Test
    public void testDownloadDataset()
    {
        String responseMsg = target.path("control/downloadspecific/berlin_de").request().get(String.class);
        assertEquals("Started downloading dataset " + "berlin_de", responseMsg);

        try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        File f = new File("json/berlin_de");
        assertTrue(f.isFile() && (f.getTotalSpace() > 0));      //does berlin_de exist and is not empty?
    }

    @Test
    public void testConvertComplete()
    {
        String responseMsg = target.path("control/convert").request().get(String.class);
        assertEquals("Started Converter", responseMsg);

        File f = new File("/output");
        long start = f.getTotalSpace();
        try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        long later = f.getTotalSpace();     //did something change in output directory?
        assertTrue(start != later);
    }

    @Test
    public void testStopConvert()
    {
        String responseMsg = target.path("control/stopconvert").request().get(String.class);
        assertEquals("Stopped Converter", responseMsg);

        boolean test = Scheduler.getConverter().getEventContainer().checkForEvent(EventNotification.EventType.stoppedConverter, EventNotification.EventSource.Converter);
        assertTrue(test == true);
    }

    @Test
    public void testPauseConverter()
    {
        String responseMsg = target.path("control/pauseconvert").request().get(String.class);
        assertEquals("Paused Converter", responseMsg);

        File f = new File("/output");
        long before = f.getTotalSpace();
        try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        long after = f.getTotalSpace();

        assertTrue(after==before);
    }

    @Test
    public void testResumeConverter()
    {
        File f = new File("/output");
        long before = f.getTotalSpace();

        String responseMsg = target.path("control/resumeconvert").request().get(String.class);
        assertEquals("Resumed Converter", responseMsg);

        try {Thread.sleep(30000);}
        catch(InterruptedException e) {}
        long after = f.getTotalSpace();

        assertTrue(after!=before);
    }

    @Test
    public void testShutdown()
    {
        String responseMsg = target.path("control/shutdown").request().get(String.class);
        assertEquals("Service shutted down.", responseMsg);
    }
}
