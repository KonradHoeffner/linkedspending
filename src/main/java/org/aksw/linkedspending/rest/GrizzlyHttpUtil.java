package org.aksw.linkedspending.rest;

import java.net.URI;
import javax.ws.rs.core.UriBuilder;
import org.aksw.linkedspending.job.Job;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

/** Sets up and handles a GrizzlyHttpServer provided by Jersey. */
public class GrizzlyHttpUtil
{
	public static final URI		baseURI	= UriBuilder.fromUri("http://localhost/").port(10010).build();
	private static HttpServer	server /* = startThisServer() */;

	/** Creates a new http server */
	public static HttpServer startThisServer()
	{
		ResourceConfig resCon = new ResourceConfig().packages("org.aksw.linkedspending.rest");
		resCon.register(Rest.class);
		resCon.register(ExceptionHandler.class);
		resCon.register(Job.class);
		// return GrizzlyHttpServerFactory.createHttpServer(baseURI, resCon);
		HttpServer server = GrizzlyHttpServerFactory.createHttpServer(baseURI, resCon);

		return server;
		// URI uri = UriBuilder.fromUri("http://myhost.com").port(10010).build();
		// server = GrizzlyHttpServerFactory.createHttpServer(uri, resCon);
		// server = new HttpServer();
		// GrizzlyHttpServerFactory.
		// NetworkListener networkListener = new NetworkListener("networkListener", "myhost.com",
		// 10011);
		// server.addListener(networkListener);
	}

//	/** Creates a new Http running on specified port. */
//	public static HttpServer startServer(int port)
//	{
//		URI uri = UriBuilder.fromUri("http://localhost/").port(port).build();
//		ResourceConfig resCon = new ResourceConfig().packages("org.aksw.linkedspending");
//		return GrizzlyHttpServerFactory.createHttpServer(uri, resCon);
//	}

	public static void shutdownGrizzly()
	{
		server.shutdown();
	}
}
