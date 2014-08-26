package io.ntropy;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


public class WebUtilities {
    private static final Logger LOG = LoggerFactory.getLogger(WebUtilities.class);
    private static final int DEFAULT_RETRIES = 0; //number of retries after a failure
    private static final int DEFAULT_TIMEOUT = 10000; //millis

    public static String sendGetRequest(String url) throws Exception {
        return sendGetRequest(url, DEFAULT_TIMEOUT, DEFAULT_RETRIES);
    }

    public static String sendGetRequest(String url, Map<String, String> headerProperties) throws Exception {
        return sendGetRequest(url, DEFAULT_TIMEOUT, DEFAULT_RETRIES, headerProperties);
    }

    public static String sendGetRequest(String url, int timeoutMillis) throws Exception {
        return sendGetRequest(url, timeoutMillis, DEFAULT_RETRIES);
    }

    public static String sendGetRequest(String url, int timeoutMillis, int maxRetries) throws Exception {
        return sendGetRequest(url, timeoutMillis, maxRetries, new HashMap<String, String>());
    }

    public static String sendGetRequest(String url, int timeoutMillis, int maxRetries, Map<String, String> headerProperties) throws Exception {
        return sendWebRequest(url, timeoutMillis, maxRetries, headerProperties, "GET", null);
    }

    public static String sendPostRequest(String url, String body) throws Exception {
        return sendPostRequest(url, DEFAULT_TIMEOUT, DEFAULT_RETRIES, body);
    }

    public static String sendPostRequest(String url, Map<String, String> headerProperties, String body) throws Exception {
        return sendPostRequest(url, DEFAULT_TIMEOUT, DEFAULT_RETRIES, headerProperties, body);
    }

    public static String sendPostRequest(String url, int timeoutMillis, String body) throws Exception {
        return sendPostRequest(url, timeoutMillis, DEFAULT_RETRIES, body);
    }

    public static String sendPostRequest(String url, int timeoutMillis, int maxRetries, String body) throws Exception {
        return sendPostRequest(url, timeoutMillis, maxRetries, new HashMap<String, String>(), body);
    }

    public static String sendPostRequest(String url, int timeoutMillis, int maxRetries, Map<String, String> headerProperties, String body) throws Exception {
        return sendWebRequest(url, timeoutMillis, maxRetries, headerProperties, "POST", body);
    }

    public static String sendPutRequest(String url, String body) throws Exception {
        return sendPutRequest(url, DEFAULT_TIMEOUT, DEFAULT_RETRIES, body);
    }

    public static String sendPutRequest(String url, Map<String, String> headerProperties, String body) throws Exception {
        return sendPutRequest(url, DEFAULT_TIMEOUT, DEFAULT_RETRIES, headerProperties, body);
    }

    public static String sendPutRequest(String url, int timeoutMillis, String body) throws Exception {
        return sendPutRequest(url, timeoutMillis, DEFAULT_RETRIES, body);
    }

    public static String sendPutRequest(String url, int timeoutMillis, int maxRetries, String body) throws Exception {
        return sendPutRequest(url, timeoutMillis, maxRetries, new HashMap<String, String>(), body);
    }

    public static String sendPutRequest(String url, int timeoutMillis, int maxRetries, Map<String, String> headerProperties, String body) throws Exception {
        return sendWebRequest(url, timeoutMillis, maxRetries, headerProperties, "PUT", body);
    }

    public static String sendDeleteRequest(String url) throws Exception {
        return sendGetRequest(url, DEFAULT_TIMEOUT, DEFAULT_RETRIES);
    }

    public static String sendDeleteRequest(String url, Map<String, String> headerProperties) throws Exception {
        return sendGetRequest(url, DEFAULT_TIMEOUT, DEFAULT_RETRIES, headerProperties);
    }

    public static String sendDeleteRequest(String url, int timeoutMillis) throws Exception {
        return sendGetRequest(url, timeoutMillis, DEFAULT_RETRIES);
    }

    public static String sendDeleteRequest(String url, int timeoutMillis, int maxRetries) throws Exception {
        return sendGetRequest(url, timeoutMillis, maxRetries, new HashMap<String, String>());
    }

    public static String sendDeleteRequest(String url, int timeoutMillis, int maxRetries, Map<String, String> headerProperties) throws Exception {
        return sendWebRequest(url, timeoutMillis, maxRetries, headerProperties, "DELETE", null);
    }

    private static String sendWebRequest(String url, int timeout, int maxRetries, Map<String,String> headerProperties, String requestType, String body) throws Exception {
        int retries = 0;
        StringBuilder sb = new StringBuilder();
        while(retries <= maxRetries) {
            HttpURLConnection connection = null;
            BufferedReader rd  = null;
            sb = new StringBuilder();
            String line = null;

            URL serverAddress = null;

            try {
                serverAddress = new URL(url);
                //set up out communications stuff
                connection = null;

                //Set up the initial connection
                connection = (HttpURLConnection)serverAddress.openConnection();
                connection.setRequestMethod(requestType);
                for(Map.Entry<String,String> requestProperty : headerProperties.entrySet()) {
                    connection.addRequestProperty(requestProperty.getKey(), requestProperty.getValue());
                }
                connection.setDoOutput(true);
                connection.setReadTimeout(timeout);

                //when the request is a GET this will be null. On a post, even if the body is empty,
                // some servers (e.g. eloqua) require a body to be written, even if it's empty.
                if(body != null) {
                    final OutputStream os = connection.getOutputStream();
                    os.write(body.getBytes());
                    os.flush();
                    os.close();
                }

                connection.connect();

                int responseCode = connection.getResponseCode();
                if(responseCode < 300) {
                    //read the result from the server
                    rd  = new BufferedReader(new InputStreamReader(connection.getInputStream()));

                    while ((line = rd.readLine()) != null) {
                        sb.append(line).append('\n');
                    }

                    LOG.debug(sb.toString());
                    return sb.toString();
                } else {
                    //read the error result from the server
                    LOG.debug("error code {}", responseCode);
                    LOG.debug("Error code {} received with presence of error stream: {}", responseCode, (connection.getErrorStream() == null) ? "false" : "true");
                    if(connection.getErrorStream() != null) {
                        rd  = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
                        while ((line = rd.readLine()) != null) {
                            sb.append(line).append('\n');
                        }
                        if (responseCode < 500) {
                            //don't retry on 400s
                            return sb.toString();
                        }
                        throw new Exception(String.format("got an error code %s accessing url %s with output %s", responseCode, url, sb.toString()));
                    } else {
                        if (responseCode < 500) {
                            //don't retry on 400s
                            return String.format("got an error code %s accessing url %s", responseCode, url);
                        }
                        throw new Exception(String.format("got an error code %s accessing url %s", responseCode, url));
                    }
                }
            } catch (Exception e) {
                retries++;
                LOG.error("failure #{} for {} request to {}", retries, requestType, url);
                Thread.sleep(500);
                if(retries > maxRetries) {
                    LOG.error(String.format("Giving up processing %s to url %s after #%s tries with error: ", requestType, url, retries), e);
                    throw e;
                }
            } finally {
                //close the connection, set all objects to null, sleep a bit and try the loop again
                connection.disconnect();
                rd = null;
                connection = null;
            }
        }
        return sb.toString();
    }

    public static Map<String,String> sendGETRequestWithStatus(String url, int timeoutMillis, int maxRetries) throws Exception {
        return sendWebRequestWithStatus(url, timeoutMillis, maxRetries, new HashMap<String, String>(), "GET", null);
    }

    private static Map<String,String> sendWebRequestWithStatus(String url, int timeout, int maxRetries, Map<String, String> headerProperties, String requestType, String body) throws Exception {
        int retries = 0;
        Map<String,String> result = Maps.newHashMap();
        StringBuilder sb = new StringBuilder();
        while(retries <= maxRetries) {
            HttpURLConnection connection = null;
            BufferedReader rd  = null;
            result = Maps.newHashMap();
            sb = new StringBuilder();
            String line = null;

            URL serverAddress = null;

            try {
                serverAddress = new URL(url);
                //set up out communications stuff
                connection = null;

                //Set up the initial connection
                connection = (HttpURLConnection)serverAddress.openConnection();
                connection.setRequestMethod(requestType);
                for(Map.Entry<String,String> requestProperty : headerProperties.entrySet()) {
                    connection.addRequestProperty(requestProperty.getKey(), requestProperty.getValue());
                }
                connection.setDoOutput(true);
                connection.setReadTimeout(timeout);

                //when the request is a GET this will be null. On a post, even if the body is empty,
                // some servers (e.g. eloqua) require a body to be written, even if it's empty.
                if(body != null) {
                    final OutputStream os = connection.getOutputStream();
                    os.write(body.getBytes());
                    os.flush();
                    os.close();
                }

                connection.connect();

                int responseCode = connection.getResponseCode();

                if(responseCode < 300) {
                    //read the result from the server
                    rd  = new BufferedReader(new InputStreamReader(connection.getInputStream()));

                    while ((line = rd.readLine()) != null) {
                        sb.append(line).append('\n');
                    }

                    LOG.debug(sb.toString());
                } else {
                    //read the error result from the server
                    rd  = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
                    while ((line = rd.readLine()) != null) {
                        sb.append(line).append('\n');
                    }
                }

                result.put("status", Integer.toString(responseCode));
                result.put("body", sb.toString());

                //only retry on 500s
                if (responseCode >= 500) {
                    throw new Exception(String.format("got an error code %s accessing url %s with output %s", responseCode, url, sb.toString()));
                } else {
                    return result;
                }
            } catch (Exception e) {
                retries++;
                LOG.error("failure #{} for {} request to {}", retries, requestType, url);
                Thread.sleep(500);
                if(retries > maxRetries) {
                    LOG.error(String.format("Giving up processing %s to url %s after #%s tries with error: ", requestType, url, retries), e);
                    throw e;
                }
            } finally {
                //close the connection, set all objects to null, sleep a bit and try the loop again
                connection.disconnect();
                rd = null;
                connection = null;
            }
        }

        return result;
    }
}
