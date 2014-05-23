package org.aksw.linkedspending.tools;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

/** Sets up and handles a GrizzlyHttp provided by Jersey. */
public class GrizzlyHttpUtil
{
    public static final URI baseURI = UriBuilder.fromUri("http://localhost/").port(10010).build();
    private static final HttpServer server = startServer();

    /** Creates a new http server */
    public static HttpServer startServer()
    {
        ResourceConfig resCon = new ResourceConfig().packages("org.aksw.linkedspending");
        return GrizzlyHttpServerFactory.createHttpServer(baseURI, resCon);
    }

    public static void shutdownGrizzly()
    {
        server.shutdown();
    }
}
