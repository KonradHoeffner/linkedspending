package org.aksw.linkedspending.tools;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

/** Sets up and handles a GrizzlyHttpServer provided by Jersey. */
public class GrizzlyHttpUtil
{
    public static final URI baseURI = UriBuilder.fromUri("http://localhost/").port(10010).build();
    private static HttpServer server /*= startThisServer()*/;

    /** Creates a new http server */
    public static /*HttpServer*/void startThisServer()
    {
        ResourceConfig resCon = new ResourceConfig().packages("org.aksw.linkedspending");
        //return GrizzlyHttpServerFactory.createHttpServer(baseURI, resCon);
        server = GrizzlyHttpServerFactory.createHttpServer(baseURI, resCon);
    }

    /** Creates a new Http running on specified port. */
    public static HttpServer startServer(int port)
    {
        URI uri = UriBuilder.fromUri("http://localhost/").port(10010).build();
        ResourceConfig resCon = new ResourceConfig().packages("org.aksw.linkedspending");
        return GrizzlyHttpServerFactory.createHttpServer(uri, resCon);
    }

    public static void shutdownGrizzly()
    {
        server.shutdown();
    }
}
