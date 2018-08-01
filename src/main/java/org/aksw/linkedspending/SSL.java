package org.aksw.linkedspending;

import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import lombok.SneakyThrows;

public class SSL
{
 @SneakyThrows
 /* Source: http://www.nakov.com/blog/2009/07/16/disable-certificate-validation-in-java-ssl-connections/ */ 
	public static void disableSslVerification()
 {
   // Create a trust manager that does not validate certificate chains
   TrustManager[] trustAllCerts = new TrustManager[] {new X509TrustManager() {
           public java.security.cert.X509Certificate[] getAcceptedIssuers() {
               return null;
           }
           public void checkClientTrusted(X509Certificate[] certs, String authType) {
           }
           public void checkServerTrusted(X509Certificate[] certs, String authType) {
           }
       }
   };

   // Install the all-trusting trust manager
   SSLContext sc = SSLContext.getInstance("SSL");
   sc.init(null, trustAllCerts, new java.security.SecureRandom());
   HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

   // Create all-trusting host name verifier
   HostnameVerifier allHostsValid = new HostnameVerifier() {
       public boolean verify(String hostname, SSLSession session) {
           return true;
       }
   };

   // Install the all-trusting host verifier
   HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
 }
}
