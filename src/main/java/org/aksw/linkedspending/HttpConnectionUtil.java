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
    static final int INITIAL_RETRY_DELAY_MS=10000;
    static final int    MAX_DELAY_MS    = 1000000; // ~ 15 min
    static final int RETRIES=5; // 10s*2^5 = 320s ~ 5 min


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
        int delayMs=INITIAL_RETRY_DELAY_MS;
        boolean delay = false;
        do {
            if (delay) {
                Thread.sleep(INITIAL_RETRY_DELAY_MS);
                delayMs=Math.max(delayMs*2,MAX_DELAY_MS);
            }
            HttpURLConnection connection = (HttpURLConnection)url.openConnection();
            int code = connection.getResponseCode();
            switch (code) {
                case HttpURLConnection.HTTP_OK:
                    log.fine(url + " **OK**");
                    return connection; // **EXIT POINT** fine, go on
                case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:
                    log.warning(url + " **gateway timeout**");
                    break;// retry
                case HttpURLConnection.HTTP_UNAVAILABLE:
                    log.warning(url + " **unavailable**");
                    throw new HttpUnavailableException(url);
                default:
                    log.severe(url + " **unknown response code: "+code+"**.");
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
