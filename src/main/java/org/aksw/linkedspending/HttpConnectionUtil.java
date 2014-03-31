package org.aksw.linkedspending;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import lombok.AllArgsConstructor;
import lombok.extern.java.Log;

@Log
@SuppressWarnings("serial")
public class HttpConnectionUtil
{
	static final int RETRY_DELAY_MS=10000;
	static final int RETRIES=2;

	@AllArgsConstructor
	public static class HttpException extends Exception
	{
		public final URL url;
		@Override public String getMessage()
		{
			return "Http error with url <"+url+">";
		}
	}
	public static class HttpUnavailableException extends HttpException {public HttpUnavailableException(URL url) {super(url);}}
	public static class HttpTimeoutException extends HttpException {public HttpTimeoutException(URL url) {super(url);}}

	public static final HttpURLConnection getConnection(URL url) throws InterruptedException, IOException, HttpTimeoutException, HttpUnavailableException
	{
		int retry = 0;
		boolean delay = false;
		do {
			if (delay) {
				Thread.sleep(RETRY_DELAY_MS);
			}
			HttpURLConnection connection = (HttpURLConnection)url.openConnection();
			switch (connection.getResponseCode()) {
				case HttpURLConnection.HTTP_OK:
					log.fine(url + " **OK**");
					return connection; // **EXIT POINT** fine, go on
				case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:	            
					log.warning(url + " **gateway timeout**");
					break;// retry
				case HttpURLConnection.HTTP_UNAVAILABLE:
					log.warning(url + "**unavailable**");
					throw new HttpUnavailableException(url);
				default:
					log.severe(url + " **unknown response code**.");
					throw new RuntimeException();
			}
			// we did not succeed with connection (or we would have returned the connection).
			connection.disconnect();
			// retry
			retry++;
			log.warning("Failed retry " + retry + "/" + RETRIES);
			delay = true;

		} while (retry < RETRIES);
		log.severe("Aborting download of dataset.");
		throw new HttpTimeoutException(url); // we retried at least once but we didn't return yet so it has to be a timeout exception
	}
}
