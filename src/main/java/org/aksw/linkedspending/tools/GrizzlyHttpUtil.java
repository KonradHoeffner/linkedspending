package org.aksw.linkedspending.tools;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

/** Sets up and handles a GrizzlyHttp provided by Jersey. */
public class GrizzlyHttpUtil {
    public static final URI baseURI = UriBuilder.fromUri("http://localhost/").port(9998).build();
    private static final HttpServer server = startServer();

    public static HttpServer startServer() {
        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        //final ResourceConfig rc = new ResourceConfig().packages("org.aksw.linkedspending");
        //ResourceConfig resCon = new ResourceConfig(Scheduler.class);
        ResourceConfig resCon = new ResourceConfig().packages("org.aksw.linkedspending");


        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        //return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI));
        return GrizzlyHttpServerFactory.createHttpServer(baseURI, resCon);
    }

    public static void shutdownGrizzly()
    {
        server.shutdown();
    }
/*
    public static void startGrizzly() throws IOException {
        //final HttpServer server = startServer();
        //System.out.println("startServer fine...");
        System.out.println(String.format("Jersey app started with WADL available at "
                + "%sapplication.wadl\n", baseURI));
    }
*/
}
