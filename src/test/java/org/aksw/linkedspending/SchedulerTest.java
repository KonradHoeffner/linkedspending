package org.aksw.linkedspending;


import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import static org.junit.Assert.assertEquals;

public class SchedulerTest {
    private HttpServer server;
    private WebTarget target;

    @Before
    public void setUp() throws Exception {
        // start the server
        server = Scheduler.startServer();
        // create the client
        Client c = ClientBuilder.newClient();

        target = c.target(Scheduler.BASE_URI);
    }

    @After
    public void tearDown() throws Exception {
        //server.stop();
        server.shutdown();
    }

    @Test
    public void testRunDownloader() {
        String responseMsg = target.path("control/downloadcomplete").request().get(String.class);
        assertEquals("Started complete download", responseMsg);
    }
}
