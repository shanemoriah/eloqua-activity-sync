package io.ntropy;

import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;

/*
Until batch access is enabled, we have to iterate through each contact and each potential activity with a GET
see: http://topliners.eloqua.com/docs/DOC-3092
 */
public class EloquaContactActivityCallback implements Callable<JSONArray>{

    private String baseURL;
    private int contactId;
    private String activityType;
    private Long startSyncDate;
    private Long endSyncDate;
    private int timeout;
    private int retries;
    private Map<String,String> headerProperties;

    private static final Logger LOG = LoggerFactory.getLogger(EloquaContactActivityCallback.class);
    private static final int MAX_COUNT = 1000;

    public EloquaContactActivityCallback(String baseURL, int contactId, String activityType, Long startSyncDate, Long endSyncDate, int timeout, int retries, Map<String, String> headerProperties) {
        this.baseURL = baseURL;
        this.contactId= contactId;
        this.activityType = activityType;
        this.startSyncDate = startSyncDate;
        this.endSyncDate = endSyncDate;
        this.timeout = timeout;
        this.retries = retries;
        this.headerProperties = headerProperties;
    }


    @Override
    public JSONArray call() throws Exception {
        return getResponses(startSyncDate, endSyncDate);
    }

    //recursive helper since responses of 1000 means we didn't get all the data and we need to break it down to a more granular request
    private JSONArray getResponses(Long curStartSyncDate, Long curEndSyncDate) throws Exception {
        String contactActivityUrl = String.format("%s/data/activities/contact/%s?type=%s&startDate=%s&endDate=%s&count=%s",
                baseURL, contactId, activityType, curStartSyncDate, curEndSyncDate, MAX_COUNT);
        String response =  WebUtilities.sendGetRequest(contactActivityUrl, timeout, retries, headerProperties);
        if(response.isEmpty()) {
            return new JSONArray();
        } else {
            JSONArray responseArray = new JSONArray(response);
            if(responseArray.length() < MAX_COUNT || curStartSyncDate.equals(curEndSyncDate)) {
                //the current array is less than the limit or our start and end date are the same and we can't further divide so we just return what we have
                return responseArray;
            } else {
                //we got back the max amount of responses which means we're missing some, break it into pieces and recurse
                Long midWayPoint = (curStartSyncDate + curEndSyncDate) / 2;
                JSONArray firstHalfResponses = getResponses(curStartSyncDate, midWayPoint);
                JSONArray secondHalfResponses = getResponses(midWayPoint + 1, curEndSyncDate);
                //then add all the second half elements to the first half and return the resulting merged array
                for(int i = 0; i < secondHalfResponses.length(); i++) {
                    firstHalfResponses.put(secondHalfResponses.get(i));
                }
                return firstHalfResponses;
            }
        }
    }
}
