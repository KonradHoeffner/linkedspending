package org.aksw.linkedspending;


import org.aksw.linkedspending.tools.GrizzlyHttpUtil;
import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import static org.junit.Assert.assertEquals;

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
        String responseMsg = target.path("control/downloadcomplete").request().get(String.class);
        assertEquals("Started complete download", responseMsg);
    }

    @Test
    public void testStopDownloader()
    {
        String responseMsg = target.path("control/stopdownload").request().get(String.class);
        assertEquals("Stopped downloading", responseMsg);
    }

    @Test
    public void testPauseDownloader()
    {
        String responseMsg = target.path("control/pausedownload").request().get(String.class);
        assertEquals("Paused Downloader", responseMsg);
    }

    @Test
    public void testResumeDownloader()
    {
        String responseMsg = target.path("control/resumedownload").request().get(String.class);
        assertEquals("Resumed Downloader", responseMsg);
    }

    @Test
    public void testDownloadDataset()
    {
        String responseMsg = target.path("control/downloadspecific/berlin_de").request().get(String.class);
        assertEquals("Started downloading dataset " + "berlin_de", responseMsg);
    }

    @Test
    public void testConvertComplete()
    {
        String responseMsg = target.path("control/convert").request().get(String.class);
        assertEquals("Started Converter", responseMsg);
    }

    @Test
    public void testStopConvert()
    {
        String responseMsg = target.path("control/stopconvert").request().get(String.class);
        assertEquals("Stopped Converter", responseMsg);
    }

    @Test
    public void testPauseConverter()
    {
        String responseMsg = target.path("control/pauseconvert").request().get(String.class);
        assertEquals("Paused Converter", responseMsg);
    }

    @Test
    public void testResumeConverter()
    {
        String responseMsg = target.path("control/resumeconvert").request().get(String.class);
        assertEquals("Resumed Converter", responseMsg);
    }

    @Test
    public void testShutdown()
    {
        String responseMsg = target.path("control/shutdown").request().get(String.class);
        assertEquals("Service shutted down.", responseMsg);
    }
}
