package org.aksw.linkedspending.scheduler;

import static org.junit.Assert.*;
import org.aksw.linkedspending.converter.Converter;
import org.aksw.linkedspending.old.JsonDownloaderOld;
import org.aksw.linkedspending.rest.GrizzlyHttpUtil;
import org.aksw.linkedspending.scheduler.Scheduler;
import org.aksw.linkedspending.tools.EventNotification;
import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.UriBuilder;

/** Tests if Scheduler reacts properly to incoming commands via REST-interface */
public class SchedulerTest
{
	private static final int	PAUSE_MILLIS	= 1000;
	private HttpServer	server;
	private WebTarget	target;
	private Client		c;

	@Before public void setUp() throws Exception
	{
		// URI uri = UriBuilder.fromUri("http://localhost/").port(10010).build();
		// start the server
		server = GrizzlyHttpUtil.startThisServer();

		// ResourceConfig resCon = new ResourceConfig().packages("org.aksw.linkedspending");
		// server = GrizzlyHttpServerFactory.createHttpServer(uri, resCon);

		// create the client
		// c = ClientBuilder.newClient();
		/*
		 * todo: there seems to be a bug with creating a client (throws nullpointerexception) at the
		 * moment, try fix this later.
		 */
		// todo: edit: might have been caused by doubly starting http server, which is fixed now.
		// Check tests again.
		//		ClientConfig clientConfig = new ClientConfig();
		c = ClientBuilder.newClient();
		target = c.target(UriBuilder.fromUri("http://localhost/").port(10010).build());

		JsonDownloaderOld.setStopRequested(false);
		JsonDownloaderOld.setPauseRequested(false);
		Converter.setPauseRequested(false);
		Converter.setStopRequested(false);
	}

	@After public void tearDown() throws Exception
	{
		// server.stop();
		Scheduler.stopDownloader();
		server.shutdown();
	}

	@Test public void testRunDownloader() throws InterruptedException
	{
		// Tests REST-functionality
		// String responseMsg =
		// target.path("http://localhost:10010/control/downloadcomplete").request().get(String.class);
		// Assert.assertEquals("Started complete download", responseMsg);
		Scheduler.runManually();
		Scheduler.runDownloader();
		// checks if files are being downloaded
		Thread.sleep(PAUSE_MILLIS);
		/*
		 * File f = new File("/json");
		 * long totalSpace = f.getTotalSpace();
		 * try{Thread.sleep(20000);}
		 * catch(InterruptedException e) {}
		 * long totalSpace2 = f.getTotalSpace();
		 * Assert.assertTrue(totalSpace < totalSpace2);
		 */
		boolean b = Scheduler.getDownloader().getEventContainer()
				.checkForEvent(EventNotification.EventType.STARTED_COMPLETE_DOWNLOAD, EventNotification.EventSource.DOWNLOADER);
		Scheduler.stopDownloader();
		Assert.assertTrue(b);
	}

	@Test public void testStopDownloader() throws InterruptedException
	{
		// String responseMsg = target.path("control/stopdownload").request().get(String.class);
		// Assert.assertEquals("Stopped downloading", responseMsg);
		Scheduler.runManually();
		Scheduler.runDownloader();
		Scheduler.stopDownloader();

		Thread.sleep(PAUSE_MILLIS);
		/*
		 * File f = new File("/json");
		 * long before = f.getTotalSpace();
		 * try{Thread.sleep(10000);}
		 * catch(InterruptedException e) {}
		 * long after = f.getTotalSpace();
		 * Assert.assertTrue(after == before);
		 */
		// if the Downloader was successfully stopped, several DownloadCallables will add
		// finishedDownloadingDataset (success == false) events after a while.
		assertTrue(Scheduler.getDownloader().getEventContainer()
				.checkForEvent(EventNotification.EventType.DOWNLOAD_STOPPED, EventNotification.EventSource.DOWNLOADER));
	}

	@Test public void testPauseDownloader() throws InterruptedException
	{
		// String responseMsg = target.path("control/pausedownload").request().get(String.class);
		// Assert.assertEquals("Paused Downloader", responseMsg);
		Scheduler.runManually();
		Scheduler.runDownloader();
		Thread.sleep(PAUSE_MILLIS);

		Scheduler.pauseDownloader();

		Thread.sleep(PAUSE_MILLIS);

		// Did the size of /json folder change after downloader has been paused?
		/*
		 * File f = new File("/json");
		 * long totalSpace = f.getTotalSpace();
		 * try{Thread.sleep(5000);}
		 * catch(InterruptedException e) {}
		 * long totalSpace2 = f.getTotalSpace();
		 * Assert.assertTrue(totalSpace == totalSpace2);
		 */
		boolean b = Scheduler.getDownloader().getEventContainer()
				.checkForEvent(EventNotification.EventType.DOWNLOAD_PAUSED, EventNotification.EventSource.DOWNLOADER);
		Assert.assertTrue(b);
		Scheduler.stopDownloader();
	}

	@Test public void testResumeDownloader()
	{
		// String responseMsg = target.path("control/resumedownload").request().get(String.class);
		// Assert.assertEquals("Resumed Downloader", responseMsg);
		Scheduler.runManually();
		Scheduler.runDownloader();
		Scheduler.pauseDownloader();
		try
		{
			Thread.sleep(1000);
		}
		catch (InterruptedException e)
		{}
		Scheduler.resumeDownload();

		/*
		 * File f = new File("/json");
		 * long totalSpace = f.getTotalSpace();
		 * try{Thread.sleep(20000);}
		 * catch(InterruptedException e) {}
		 * long totalSpace2 = f.getTotalSpace();
		 * Assert.assertTrue(totalSpace < totalSpace2);
		 */

		boolean b = Scheduler.getDownloader().getEventContainer()
				.checkForEvent(EventNotification.EventType.DOWNLOAD_RESUMED, EventNotification.EventSource.DOWNLOADER);
		Assert.assertTrue(b);
		Scheduler.stopDownloader();
	}

	@Test public void testDownloadDataset()
	{
		// String responseMsg =
		// target.path("control/downloadspecific/berlin_de").request().get(String.class);
		// Assert.assertEquals("Started downloading dataset " + "berlin_de", responseMsg);
		Scheduler.runManually();
		Scheduler.downloadDataset("berlin_de");
		try
		{
			Thread.sleep(1000);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}

		/*
		 * try {Thread.sleep(30000);}
		 * catch(InterruptedException e) {}
		 * File f = new File("json/berlin_de");
		 * Assert.assertTrue(f.isFile() && (f.getTotalSpace() > 0)); //does berlin_de exist and is
		 * not empty?
		 */
		boolean b = Scheduler.getDownloader().getEventContainer()
				.checkForEvent(EventNotification.EventType.STARTED_SINGLE_DOWNLOAD, EventNotification.EventSource.DOWNLOADER);
		Assert.assertTrue(b);
		Scheduler.stopDownloader();
	}

	@Test public void testConvertComplete()
	{
		// String responseMsg = target.path("control/convert").request().get(String.class);
		// Assert.assertEquals("Started Converter", responseMsg);
		Scheduler.runManually();
		Scheduler.runConverter();

		try
		{
			Thread.sleep(6000);
		}
		catch (InterruptedException e)
		{}
		/*
		 * File f = new File("/output");
		 * long start = f.getTotalSpace();
		 * try {Thread.sleep(30000);}
		 * catch(InterruptedException e) {}
		 * long later = f.getTotalSpace(); //did something change in output directory?
		 * Assert.assertTrue(start != later);
		 */

		boolean b = Scheduler.getConverter().getEventContainer()
				.checkForEvent(EventNotification.EventType.STARTED_CONVERTING_COMPLETE, EventNotification.EventSource.CONVERTER);
		Assert.assertTrue(b);
	}

	@Test public void testStopConvert()
	{
		// String responseMsg = target.path("control/stopconvert").request().get(String.class);
		// Assert.assertEquals("Stopped Converter", responseMsg);
		Scheduler.runManually();
		Scheduler.runConverter();
		Scheduler.stopConverter();

		try
		{
			Thread.sleep(5000);
		}
		catch (InterruptedException e)
		{}

		boolean b = Scheduler.getConverter().getEventContainer()
				.checkForEvent(EventNotification.EventType.STOPPED_CONVERTER, EventNotification.EventSource.CONVERTER);
		Assert.assertTrue(b);
	}

	@Test public void testPauseConverter()
	{
		// String responseMsg = target.path("control/pauseconvert").request().get(String.class);
		// Assert.assertEquals("Paused Converter", responseMsg);
		Scheduler.runManually();
		Scheduler.runConverter();
		Scheduler.pauseConverter();

		try
		{
			Thread.sleep(PAUSE_MILLIS);
		}
		catch (InterruptedException e)
		{}
		/*
		 * File f = new File("/output");
		 * long before = f.getTotalSpace();
		 * try {Thread.sleep(30000);}
		 * catch(InterruptedException e) {}
		 * long after = f.getTotalSpace();
		 * Assert.assertTrue(after == before);
		 */

		boolean b = Scheduler.getConverter().getEventContainer()
				.checkForEvent(EventNotification.EventType.PAUSED_CONVERTER, EventNotification.EventSource.CONVERTER);
		Assert.assertTrue(b);
	}

	@Test public void testResumeConverter()
	{
		// File f = new File("/output");
		// long before = f.getTotalSpace();

		Scheduler.runManually();
		Scheduler.runConverter();
		Scheduler.pauseConverter();
		try
		{
			Thread.sleep(PAUSE_MILLIS);
		}
		catch (InterruptedException e)
		{}
		Scheduler.resumeConverter();
		// String responseMsg = target.path("control/resumeconvert").request().get(String.class);
		// Assert.assertEquals("Resumed Converter", responseMsg);

		try
		{
			Thread.sleep(PAUSE_MILLIS);
		}
		catch (InterruptedException e)
		{}
		/*
		 * try {Thread.sleep(30000);}
		 * catch(InterruptedException e) {}
		 * long after = f.getTotalSpace();
		 * Assert.assertTrue(after != before);
		 */

		assertTrue(Scheduler.getConverter().getEventContainer()
				.checkForEvent(EventNotification.EventType.RESUMED_CONVERTER, EventNotification.EventSource.CONVERTER));
	}

	@Test public void testShutdown()
	{
		// String responseMsg = target.path("control/shutdown").request().get(String.class);
		// Assert.assertEquals("Service shutted down.", responseMsg);
	}
}
