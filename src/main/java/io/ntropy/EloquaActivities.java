package io.ntropy;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/*resources:
    file:///home/shane/Downloads/BULK%20API%20Guide%201.0%20(1).pdf
    https://secure.eloqua.com/api/docs/Dynamic/Bulk/1.0/Reference.aspx
    https://secure.eloqua.com/api/docs/Dynamic/Bulk/1.0/Export.aspx
    http://topliners.eloqua.com/docs/DOC-6918 (2.0 API docs)
*/

public class EloquaActivities {
    private static final Logger LOG = LoggerFactory.getLogger(EloquaActivities.class);

    private String eloquaCompanyName;
    private String eloquaUsername;
    private String eloquaPwd;
    private String eloquaActivitiesSegment;
    private Boolean activitiesForAllContacts;

    private Map<String,String> headerProperties;

    //configs for processing activities, num of threads to spawn and number of contacts to process per batch
    private static final int NUM_THREADS = 16;
    private static final int ACTIVITY_PROCESSING_BATCH = 10000;

    //default segment has 2 days in it
    private static final int MAX_OPERATION_RETRIES = 3;
    private static final String DEFAULT_ACTIVITIES_SEGMENT = "6SenseActivities";

    private static final String CONTACT_OBJECT_NAME = "contacts";
    private static final String ACTIVITIES_OBJECT_NAME = "activities";

    private static final int RESULTS_PER_PAGE = 10000;

    //these are default values but might change after looking up endpoints from login
    private String bulkV2URL;
    private String restV1URL;
    private String restV2URL;

    private static final int MAX_INTERVALS_TO_WAIT_FOR_SEGMENT_REFRESH = 60;
    private static final Long MAX_MILLIS_TO_WAIT_FROM_POLLING = 2 * 60 * 60 * 1000L; //2 hours at most
    private static final int MAX_HOURS_FOR_ACTIVITY_SYNC = 12;

    private static final int SECONDS_TO_AUTO_DELETE = 3600;
    private static final int SECONDS_TO_RETAIN_DATA = 3600;

    private static final int MILLIS_TO_WAIT_FOR_SEGMENT_REFRESH = 10000;
    private static final int MILLIS_TO_WAIT_FOR_SEGMENT_POST = 60 * 1000;
    private static final int MILLIS_TO_WAIT_FOR_SYNC_POST = 5 * 60 * 1000;
    private static final int MILLIS_TO_WAIT_FOR_ACTIVITY = 10 * 1000;
    private static final int MILLIS_TO_WAIT_FOR_POLL = 30 * 1000;
    private static final int MILLIS_TO_WAIT_BETWEEN_POLLS = 5000;
    private static final int MILLIS_TO_WAIT_FOR_EXPORT = 30 * 1000;
    private static final int WEBREQUEST_RETRIES = 5;
    private static final int ACTIVITY_WEBREQUEST_RETRIES = 1;

    private static final String ID_COL = "Id";
    private static final String CONTACT_ID_DEFINITION = "{{Contact.Id}}";

    private static final List<String> BULK_ACTIVITY_TYPES = Lists.newArrayList("EmailOpen", "EmailSend", "EmailClickthrough",
            "Subscribe", "Unsubscribe", "FormSubmit", "WebVisit", "PageView", "Bounceback");
    private static final List<String> ACTIVITY_TYPES = Lists.newArrayList("emailOpen","emailSend","emailClickThrough",
            "emailSubscribe","emailUnsubscribe","formSubmit","webVisit");//,"campaignMembership");

    private static Map<String, JSONObject> activityFieldsMapping;

    private static Map<String, Integer> bulkActivityTypeCount = Maps.newHashMap();
    private static Map<String, Integer> activityTypeCount = Maps.newHashMap();

    public static final org.joda.time.format.DateTimeFormatter DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmm").withZoneUTC();
    public static final String DateHourMinSecFormat = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter DateHourMinSecFormatter = DateTimeFormat.forPattern(DateHourMinSecFormat).withZoneUTC();

    private static class CmdLineParams {
        @Parameter(description = "eloqua company name", names = "-eloquaCompanyName", required = true)
        public String eloquaCompanyName;

        @Parameter(description = "eloqua username", names = "-eloquaUsername", required = true)
        public String eloquaUsername;

        @Parameter(description = "eloqua pwd", names = "-eloquaPwd", required = true)
        public String eloquaPwd;

        @Parameter(description = "the fully qualified segment uri for the 6SenseActivities segment (e.g. /contact/segment/1234)", names = "-activitiesSegmentURI", required = true)
        public String activitiesSegmentURI;

        @Parameter(description = "flag to sync activities on all contacts rather than segment members", names = "-activitiesForAllContacts", required = false)
        public Boolean activitiesForAllContacts = false;

    }

    public EloquaActivities(String eloquaCompanyName, String eloquaUsername, String eloquaPwd,
                            String eloquaActivitiesSegment, Boolean activitiesForAllContacts) {
        this.eloquaCompanyName = eloquaCompanyName;
        this.eloquaUsername = eloquaUsername;
        this.eloquaPwd = eloquaPwd;
        this.eloquaActivitiesSegment = eloquaActivitiesSegment;
        this.activitiesForAllContacts = activitiesForAllContacts;
    }

    public static void main(String[] args) {
        CmdLineParams cmdLineParams = new CmdLineParams();
        new JCommander(cmdLineParams, args);

        EloquaActivities eloquaActivities = new EloquaActivities(cmdLineParams.eloquaCompanyName,
                cmdLineParams.eloquaUsername, cmdLineParams.eloquaPwd, cmdLineParams.activitiesSegmentURI,
                cmdLineParams.activitiesForAllContacts);

        try {
            eloquaActivities.setup();

            //setup runs for previous 1 day
            Long startDate = DateTime.now().plusDays(-1).getMillis();
            Long endDate = DateTime.now().getMillis();

            //bulk api call and related code is functional here but commented out.
            // There are bugs and issues with the current 2.0 bulk api so we'll wait for them to be resolved before switching over.
            eloquaActivities.runBulkActivities(startDate, endDate);
            eloquaActivities.runActivities(startDate, endDate);
        } catch (Exception ex) {
            LOG.error("FATAL EXCEPTION: ", ex);
            System.exit(1);
        } catch (Error er) {
            LOG.error("FATAL ERROR: ", er);
            System.exit(1);
        } catch (Throwable t) {
            LOG.error("FATAL THROWABLE: ", t);
            System.exit(1);
        }
        System.exit(0);
    }

    private void setup() throws Exception {
        activityFieldsMapping = EloquaActivityFields.getActivityFields();
        apiSetup();
    }

    private void apiSetup() throws Exception {
        headerProperties = EloquaUtils.getHeaderProperties(eloquaCompanyName, eloquaUsername, eloquaPwd);
        String baseURL = EloquaUtils.getBaseEndpoint(headerProperties);
        restV1URL =  EloquaUtils.getRESTEndpoint(baseURL,1);
        restV2URL =  EloquaUtils.getRESTEndpoint(baseURL,2);
        bulkV2URL =  EloquaUtils.getBulkEndpoint(baseURL,2);
    }

    private void removeExportRequest(String exportURI) throws Exception {
        String exportDeletionURL = String.format("%s%s", bulkV2URL, exportURI);
        LOG.debug("deleting our temporary export of contacts with url {}", exportDeletionURL);
        String deletionResult = WebUtilities.sendDeleteRequest(exportDeletionURL, MILLIS_TO_WAIT_FOR_SEGMENT_POST, 3, headerProperties);
        LOG.debug("export deletion result {}", deletionResult);
    }

    public void runBulkActivities(Long startDate, Long endDate) throws Throwable {
        String objectName = ACTIVITIES_OBJECT_NAME;
        //check the db to see if we've done a sync before on this object and if so what the last sync time was
        Long previousMaxDateLong = startDate;
        Long endDateLong = endDate;

        String previousMaxDateStr = DateHourMinSecFormatter.withZoneUTC().print(previousMaxDateLong);
        String endDateStr = DateHourMinSecFormatter.withZoneUTC().print(endDateLong);

        LOG.debug("previous max timestamp {} turned into previousMaxDateStr {}", previousMaxDateLong, previousMaxDateStr);

        for(String activityType : BULK_ACTIVITY_TYPES) {
            int attempts = 0;
            while (true) {
                try {
                    //tell the api what query you want to run
                    LOG.info("kicking off export for activities of type {} created between {} and {}", activityType, previousMaxDateStr, endDateStr);
                    String exportURI = kickoffBulkActivityExport(activityType, activityFieldsMapping.get(activityType), previousMaxDateStr, endDateStr);
                    //start running the query and syncing to server-side staging
                    LOG.info("kicking off sync");
                    String syncedInstanceURI = kickoffSync(exportURI);
                    //wait until data is staged successfully
                    LOG.info("polling for completion");
                    if(pollUntilFinished(syncedInstanceURI)) {
                        //sync to staging is complete, now export
                        LOG.info("retrieving data");
                        retrieveBulkActivityData(exportURI, activityType);
                        LOG.info("found the following activity counts {}", bulkActivityTypeCount);
                        //finished processing this segment, break out of our while loop
                        break;
                    } else {
                        LOG.error("failure after polling sync for {}!", objectName);
                        throw new Exception(String.format("unknown failure in sync while polling for %s! dying!", objectName));
                    }
                } catch (Throwable t) {
                    attempts++;
                    LOG.error(String.format("error #%s trying to process activities between %s and %s", attempts, previousMaxDateStr, endDateStr), t);
                    if(attempts > MAX_OPERATION_RETRIES) {
                        LOG.error("encountered {} errors processing activities between {} and {}", attempts, previousMaxDateStr, endDateStr);
                        throw t;
                    }
                }
            }
        }

        LOG.info("bulk api returned us the following activities per type: {}", bulkActivityTypeCount.toString());
        LOG.info("individual api returned us the following activities per type: {}", activityTypeCount.toString());
        return;
    }

    public void runActivities(Long startDate, Long endDate) throws Exception {
        //check the db to see if we've done a sync before on this object and if so what the last sync time was
        Long previousMaxDateLong = startDate;
        Long endDateToSync = endDate/1000;
        List<Integer> contactsWithActivity;
        if(activitiesForAllContacts) {
            contactsWithActivity = getAllContactIds();
        } else {
            contactsWithActivity = getContactsFromDefaultSegmentWithRecentActivity();
        }
        previousMaxDateLong = previousMaxDateLong / 1000;

        LOG.info("getting data for {} contacts between {} and {}", contactsWithActivity.size(), DateTimeFormatter.print(previousMaxDateLong * 1000), DateTimeFormatter.print(endDateToSync * 1000));
        getActivitiesForContacts(contactsWithActivity, previousMaxDateLong, endDateToSync);

        LOG.info("bulk api returned us the following activities per type: {}", bulkActivityTypeCount.toString());
        LOG.info("individual api returned us the following activities per type: {}", activityTypeCount.toString());
    }

    private List<Integer> getAllContactIds() throws Exception {
        //tell the api what query you want to run
        LOG.debug("kicking off export for all contact ids");
        String exportURI = kickoffAllContactsExport();
        LOG.debug("got back export uri {}", exportURI);
        List<Integer> contactIds = retrieveContactsFromExportURI(exportURI);
        //delete export to clean up after ourselves
        removeExportRequest(exportURI);
        return contactIds;
    }

    //gets contacts from the defaultSegment
    private List<Integer> getContactsFromDefaultSegmentWithRecentActivity() throws Exception {
        String activitySegmentURI = (eloquaActivitiesSegment == null) ? getActivitySegmentURI(DEFAULT_ACTIVITIES_SEGMENT) : eloquaActivitiesSegment;
        return getContactsFromSegmentWithRecentActivity(activitySegmentURI);
    }

    private List<Integer> getContactsFromSegmentWithRecentActivity(String activitySegmentURI) throws Exception {
        JSONObject fields = new JSONObject();
        fields.put(ID_COL, CONTACT_ID_DEFINITION);

        //strip out the id from the uri returned
        Integer segmentId = Integer.parseInt(activitySegmentURI.substring(activitySegmentURI.lastIndexOf('/') + 1));
        LOG.debug("getting refreshed segment count for id {}", segmentId);
        Integer contactCount = refreshSegmentAndGetCount(segmentId);
        LOG.info("kicking off activities segment export for {} to grab the newly refreshed {} contacts from the segment", activitySegmentURI, contactCount);
        String exportURI = kickoffActivitiesSegmentExport(CONTACT_OBJECT_NAME, fields, activitySegmentURI);
        List<Integer> contactIds = retrieveContactsFromExportURI(exportURI);
        LOG.info("successfully retrieved {} contacts", contactIds.size());
        //delete export so we can delete segment after
        removeExportRequest(exportURI);
        return contactIds;
    }

    private List<Integer> retrieveContactsFromExportURI(String exportURI) throws Exception {
        //start running the query and syncing to server-side staging
        LOG.debug("kicking off contacts sync");
        String syncedInstanceURI = kickoffSync(exportURI);
        //wait until data is staged successfully
        LOG.debug("polling for completion on syncedInstanceUri {}", syncedInstanceURI);
        if(pollUntilFinished(syncedInstanceURI)) {
            //sync to staging is complete, now export
            LOG.debug("retrieving data");
            return retrieveContacts(exportURI);
        } else {
            LOG.error("failure in getting contacts with activity! dying!");
            throw new Exception(String.format("unknown failure getting contacts with activity while polling! dying!"));
        }
    }

    private String getActivitySegmentURI(String segmentName) throws Exception {
        //inform the api to sync the results of the query we just issued into a staging environment
        String searchUrl = String.format("%s/contacts/segments?q=name='%s'", bulkV2URL, URLEncoder.encode(segmentName, "UTF-8"));
        LOG.debug("getting activity segments using url {}", searchUrl);
        //the result gives us a new url with an id of the current sync process for status querying
        String result = WebUtilities.sendGetRequest(searchUrl, MILLIS_TO_WAIT_FOR_SYNC_POST, WEBREQUEST_RETRIES, headerProperties);

        LOG.debug("activity segment result for segment {} from url {} was {} prior to refresh", segmentName, searchUrl, result.toString());
        JSONObject responseObject = new JSONObject(result);
        if(responseObject.getInt("totalResults") == 0) {
            LOG.info("activity segment {} not found", segmentName);
            return null;
        } else if(responseObject.getInt("totalResults") != 1) {
            throw new Exception(String.format("Did not find one and only one result for activities segment %s", segmentName));
        } else {
            JSONArray items = responseObject.getJSONArray("items");
            return items.getJSONObject(0).getString("uri");
        }
    }

    //this call should refresh the segment and return the new count
    private Integer refreshSegmentAndGetCount(Integer segmentId) throws Exception {
        //construct the call to enqueue a segment refresh
        String segmentQueueUrl = String.format("%s/assets/contact/segment/queue/%s", restV2URL, segmentId);
        //the result gives us a queue status with a started at time
        //clone the headerProperties and set content-length to 0, since it seems required, along with an empty, but present, POST body
        Map<String,String> headerPropsWithContentLength = Maps.newHashMap(headerProperties);
        headerPropsWithContentLength.put("Content-Length", "0");
        //enqueue the refresh, we'll have to busy wait and check the last updated time to find out when it finishes
        LOG.debug("submitting refresh POST to {}", segmentQueueUrl);
        String queueResult = WebUtilities.sendPostRequest(segmentQueueUrl, MILLIS_TO_WAIT_FOR_SYNC_POST, WEBREQUEST_RETRIES, headerPropsWithContentLength, "");
        LOG.debug("refresh POST returned {}", queueResult);
        //grab the time we started the refresh, we'll know it's completed when the last updated time is greater than or equal to this enqueue time
        JSONObject resultJSON = new JSONObject(queueResult);
        Long queuedAt = resultJSON.getLong("queuedAt");

        int refreshIntervals = 0;
        Long lastUpdatedAt = 0L;
        String segmentCountUrl = String.format("%s/assets/contact/segment/%s/count", restV2URL, segmentId);
        while(lastUpdatedAt < queuedAt) {
            LOG.debug("waiting for segment to update, queued at {} and current last updated at {}", queuedAt, lastUpdatedAt);
            //sleep for MILLIS_TO_WAIT_FOR_SEGMENT_REFRESH millisecond to allow refresh processing to work
            Thread.sleep(MILLIS_TO_WAIT_FOR_SEGMENT_REFRESH);
            String countResult = WebUtilities.sendGetRequest(segmentCountUrl, MILLIS_TO_WAIT_FOR_SYNC_POST, WEBREQUEST_RETRIES, headerProperties);
            LOG.debug("count result {}", countResult);
            resultJSON = new JSONObject(countResult);
            try {
                lastUpdatedAt = Long.parseLong(resultJSON.getString("lastCalculatedAt"));
            } catch (NumberFormatException nfe) {
                lastUpdatedAt = 0L;
            }

            if(++refreshIntervals > MAX_INTERVALS_TO_WAIT_FOR_SEGMENT_REFRESH) {
                throw new Exception(String.format("Waited for %s seconds but didn't get a successful update of the activities segment", MAX_INTERVALS_TO_WAIT_FOR_SEGMENT_REFRESH*MILLIS_TO_WAIT_FOR_SEGMENT_REFRESH/1000));
            }
        }

        return Integer.parseInt(resultJSON.getString("count"));
    }


    private void getActivitiesForContacts(List<Integer> contactsWithActivity, Long startSyncDate, Long endSyncDate) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
        Integer contact;
        List<Integer> batchContacts = Lists.newArrayList();
        //run through each contact listed as having activity
        for(int i = 0; i < contactsWithActivity.size(); i++) {
            contact = contactsWithActivity.get(i);
            batchContacts.add(contact);

            if((i != 0 && i % ACTIVITY_PROCESSING_BATCH == 0) || i == contactsWithActivity.size() - 1) {
                processActivityContactBatch(batchContacts, pool, startSyncDate, endSyncDate);
                batchContacts.clear();
                LOG.info("finished processing activities for {} contacts out of {}", i, contactsWithActivity.size());
            }
        }
    }

    private void processActivityContactBatch(List<Integer> contactBatch, ExecutorService pool, Long startSyncDate, Long endSyncDate) throws Exception {
        int minContactId = contactBatch.get(0);
        int maxContactId = contactBatch.get(contactBatch.size() - 1);
        LOG.info("starting to process activities with contact Ids between {} and {}", minContactId, maxContactId);

        Stopwatch sw = Stopwatch.createStarted();
        int attempts = 0;
        Throwable lastException = null;
        while(attempts < MAX_OPERATION_RETRIES) {
            try {
                List<Future<JSONArray>> futures = Lists.newArrayList();
                int batchActivities = 0;
                int maxActivitiesLength = 0;
                for(Integer contact : contactBatch) {
                    //run through each activity type that they might have had and submit a call
                    for(String type : ACTIVITY_TYPES) {
                        Future<JSONArray> future = pool.submit(
                                new EloquaContactActivityCallback(restV1URL, contact, type, startSyncDate, endSyncDate,
                                        MILLIS_TO_WAIT_FOR_ACTIVITY, ACTIVITY_WEBREQUEST_RETRIES, headerProperties));
                        futures.add(future);
                    }
                }

                int errors = 0;
                int processed = 0;
                LOG.info("beginning to process {} futures for batch of size {} with {} activities each", futures.size(), contactBatch.size(), ACTIVITY_TYPES.size());
                JSONArray activityRecord = null;
                //add a failsafe timeout on the future to make sure we don't wait much longer than the call could possibly take
                int futureTimeout = (MILLIS_TO_WAIT_FOR_ACTIVITY / 1000) * (ACTIVITY_WEBREQUEST_RETRIES + 1);
                for(Future<JSONArray> future : futures) {
                    try {
                        activityRecord = future.get(futureTimeout, TimeUnit.SECONDS);
                        int activityCnt = processActivities(activityRecord);
                        batchActivities += activityCnt;
                        if(activityCnt > maxActivitiesLength) {
                            maxActivitiesLength = activityCnt;
                        }
                    } catch (Exception e) {
                        lastException = new Exception(String.format("problem processing json %s",activityRecord),e);
                        if(++errors * 100 > futures.size()) {
                            //more than 1% errors, throw along whatever exception we found to restart the batch
                            LOG.error("more than 1% of the records resulted in error, retrying the batch");
                            throw lastException;
                        } else if (sw.elapsed(TimeUnit.HOURS) > MAX_HOURS_FOR_ACTIVITY_SYNC) {
                            throw new Exception(String.format("Erroring out since activity sync batch has taken more than %s hours", MAX_HOURS_FOR_ACTIVITY_SYNC));
                        }
                    }

                    if(++processed % 1000 == 0) {
                        LOG.info("processed {} total activities from {} futures with {} errors", batchActivities, processed, errors);
                        LOG.info("current activity breakdown {}", activityTypeCount.toString());
                    }
                }

                LOG.info("processed {} activities for {} contacts ({} of which resulted in errors) between {} and {} with max activity length {}", batchActivities, contactBatch.size(), errors, minContactId, maxContactId, maxActivitiesLength);
                return;
            } catch (Throwable ex) {
                attempts++;
                lastException = ex;
                if(sw.elapsed(TimeUnit.HOURS) > MAX_HOURS_FOR_ACTIVITY_SYNC) break;
                LOG.error(String.format("exception/error #%s processing contact batch %s-%s", attempts, minContactId, maxContactId), ex);
            }
        }

        //getting out of the while loop means we errored out, break the process
        throw new Exception(String.format("FAILING...reached max of %s errors trying to get activities for contacts between %s and %s",
                MAX_OPERATION_RETRIES, minContactId, maxContactId), lastException);

    }

    private int processActivities(JSONArray result) throws JSONException, IOException {
        JSONObject jsonResult;
        String activityType;
        //non-empty results return an array of activities, grab the array and process each one
        for(int i = 0; i < result.length(); i++) {
            jsonResult = result.getJSONObject(i);
            activityType = jsonResult.getString("activityType");
            if(activityTypeCount.containsKey(activityType)) {
                activityTypeCount.put(activityType, activityTypeCount.get(activityType) + 1);
            } else {
                activityTypeCount.put(activityType, 1);
            }
        }
        return result.length();
    }

    private String kickoffBulkActivityExport(String activityType, JSONObject fields, String createdAfter, String createdBefore) throws Exception {
        //configure the export request with the proper filter, fields and other variables
        JSONObject exportBody = createBasicExportBody(ACTIVITIES_OBJECT_NAME, fields);

        exportBody.put("filter", createActivityDateBetweenFilter(activityType, createdAfter, createdBefore));

        return getExportURI(exportBody,ACTIVITIES_OBJECT_NAME,null);
    }

    private String kickoffActivitiesSegmentExport(String objectName, JSONObject fields, String activityURI) throws Exception {
        //configure the export request with the proper filter, fields and other variables
        JSONObject exportBody = createBasicExportBody(objectName, fields);

        String segmentId = activityURI.substring(activityURI.lastIndexOf('/') + 1);
        exportBody.put("filter", String.format("EXISTS('{{ContactSegment[%s]}}')", segmentId));

        return getExportURI(exportBody, objectName, null);
    }

    private String kickoffAllContactsExport() throws Exception {
        JSONObject fields = new JSONObject();
        fields.put(ID_COL, CONTACT_ID_DEFINITION);

        //configure the export request with the proper filter, fields and other variables
        JSONObject exportBody = createBasicExportBody(CONTACT_OBJECT_NAME, fields);
        return getExportURI(exportBody, CONTACT_OBJECT_NAME, null);
    }

    private JSONObject createBasicExportBody(String objectName, JSONObject fields) throws Exception {
        JSONObject exportBody = new JSONObject();
        exportBody.put("name", String.format("%s_export", objectName));
        //retain data for 1 hour while we access it
        exportBody.put("secondsToAutoDelete", SECONDS_TO_AUTO_DELETE);
        exportBody.put("secondsToRetainData", SECONDS_TO_RETAIN_DATA);
        if(fields != null) exportBody.put("fields", fields);
        return exportBody;
    }

    //want a date filter between a start and end date like :
    //"filter" : "’{{Activity.Type}}’ = ’EmailSend’ AND ’{{Activity.CreatedAt}}’ > ’2013-01-01’ AND ’{{Activity.CreatedAt}}’ < ’2013-12-31'"
    private String createActivityDateBetweenFilter(String activityType, String createdAfter, String createdBefore) throws Exception {
        StringBuilder filter = new StringBuilder();
        filter.append("'{{Activity.Type}}' = '").append(activityType).append("'");
        filter.append(" AND '{{Activity.CreatedAt}}' > '").append(createdAfter).append("'");
        if(createdBefore != null) {
            filter.append(" AND '{{Activity.CreatedAt}}' < '").append(createdBefore).append("'");
        }
        return filter.toString();
    }

    private String getExportURI(JSONObject exportBody, String objectName, Integer customObjectId) throws Exception {
        String exportURL;
        if(customObjectId == null) {
            exportURL = String.format("%s/%s/exports", bulkV2URL, objectName);
        } else {
            exportURL = String.format("%s/customObjects/%s/exports", bulkV2URL, customObjectId);
        }
        String body = exportBody.toString();
        LOG.debug("writing export post request with body : {}", body);
        String result = WebUtilities.sendPostRequest(exportURL, headerProperties, body);
        LOG.debug("export result was {}", result);
        JSONObject responseObject = new JSONObject(result);
        return responseObject.getString("uri");
    }

    private String kickoffSync(String exportUri) throws Exception {
        //inform the api to sync the results of the query we just issued into a staging environment
        String syncURL = String.format("%s/syncs", bulkV2URL);

        //all we pass in is the url from the previous call
        JSONObject syncBody = new JSONObject();
        syncBody.put("syncedInstanceUri", exportUri);
        String body = syncBody.toString();
        LOG.debug("writing sync post request to url {} with body : {}", syncURL, body);

        //the result gives us a new url with an id of the current sync process for status querying
        String result = WebUtilities.sendPostRequest(syncURL, MILLIS_TO_WAIT_FOR_SYNC_POST, WEBREQUEST_RETRIES, headerProperties, body);
        LOG.debug("sync result was {}", result);
        JSONObject responseObject = new JSONObject(result);
        return responseObject.getString("uri");
    }

    private boolean pollUntilFinished(String syncedInstanceURI) throws Exception {
        //hit the status url on an interval until the data is ready to be exported
        String syncPollingURL;
        syncPollingURL = String.format("%s%s", bulkV2URL, syncedInstanceURI);
        Long totalMillisWaited = 0L;
        while(true) {
            String result = WebUtilities.sendGetRequest(syncPollingURL, MILLIS_TO_WAIT_FOR_POLL, WEBREQUEST_RETRIES, headerProperties);
            LOG.info("poll result {}", result);
            JSONObject jsonResult = new JSONObject(result);
            String status = jsonResult.getString("status");
            if(status.equals("pending") || status.equals("active")) {
                Thread.sleep(MILLIS_TO_WAIT_BETWEEN_POLLS);
                totalMillisWaited += MILLIS_TO_WAIT_BETWEEN_POLLS;
                if(totalMillisWaited > MAX_MILLIS_TO_WAIT_FROM_POLLING) {
                    LOG.error("giving up on sync with url {} after {} seconds waiting", syncedInstanceURI, totalMillisWaited/1000);
                    return false;
                } else {
                    LOG.info("status is {}, polling for total {} seconds", status, totalMillisWaited/1000);
                }
            } else if(status.equals("warning") || status.equals("error")) {
                LOG.error("sync with url {} finished with a status of {} status result {}", syncedInstanceURI, status, jsonResult);
                //on failure try to get the logs for the failure with more info on what went wrong
                String statusLogsURL = String.format("%s/logs", syncPollingURL);
                JSONObject logResult = new JSONObject(WebUtilities.sendGetRequest(statusLogsURL, MILLIS_TO_WAIT_FOR_POLL, WEBREQUEST_RETRIES, headerProperties));
                LOG.error("full result: {}", logResult.toString());
                return false;
            } else if(status.equals("success")){
                return true;
            } else {
                throw new Exception(String.format("unknown status %s", status));
            }
        }
    }

    private void retrieveBulkActivityData(String uri, String activityType) throws Exception {
        Integer totalRecords = null;
        int retrievedRecords = 0;
        while(totalRecords == null || retrievedRecords < totalRecords) {
            String retrievalURL = String.format("%s%s/data?offset=%s&limit=%s", bulkV2URL, uri, retrievedRecords, RESULTS_PER_PAGE);
            LOG.debug("retrieving with {}", retrievalURL);
            String results = WebUtilities.sendGetRequest(retrievalURL, MILLIS_TO_WAIT_FOR_EXPORT, WEBREQUEST_RETRIES, headerProperties);
            JSONObject resultsJSON = new JSONObject(results);

            if(totalRecords == null) {
                totalRecords = resultsJSON.getInt("totalResults");
                bulkActivityTypeCount.put(activityType, totalRecords);
                if(totalRecords == 0) {
                    //no entries for this query, we'll still write the file for consistency
                    LOG.info("no results for {} ", ACTIVITIES_OBJECT_NAME);
                    return;
                }
            } else {
                LOG.debug("{} total records with {} retrieved so far", totalRecords, retrievedRecords);
            }

            JSONArray values = resultsJSON.getJSONArray("items");

            retrievedRecords += values.length();
            if(retrievedRecords % RESULTS_PER_PAGE*10 == 0) {
                LOG.info("retrieved {} of {} records for {}", retrievedRecords, totalRecords, ACTIVITIES_OBJECT_NAME);
            }
        }
    }

    private List<Integer> retrieveContacts(String uri) throws Exception {
        List<Integer> contactsToRetrieve = Lists.newArrayList();
        Integer totalRecords = null;
        int retrievedRecords = 0;
        while(totalRecords == null || retrievedRecords < totalRecords) {
            String retrievalURL = String.format("%s%s/data?offset=%s&limit=%s", bulkV2URL, uri, retrievedRecords, RESULTS_PER_PAGE);
            LOG.debug("retrieving with {}", retrievalURL);
            String results = WebUtilities.sendGetRequest(retrievalURL, MILLIS_TO_WAIT_FOR_EXPORT, WEBREQUEST_RETRIES, headerProperties);
            JSONObject resultsJSON;
            try {
                resultsJSON = new JSONObject(results);
            } catch (JSONException je) {
                LOG.info("retrieval error with result {}", results);
                throw je;
            }

            if(totalRecords == null) totalRecords = resultsJSON.getInt("totalResults");
            if(totalRecords == 0) {
                //no entries for this query
                LOG.info("no results for contacts with activity query");
                return contactsToRetrieve;
            }

            JSONArray items = resultsJSON.getJSONArray("items");

            JSONObject jsonObject;
            Integer contactId = null;
            for(int i = 0; i < items.length(); i++) {
                jsonObject = items.getJSONObject(i);
                contactId = jsonObject.getInt(ID_COL);
                contactsToRetrieve.add(contactId);
            }

            retrievedRecords += items.length();
            LOG.info("retrieved {} contacts with last one {}", contactsToRetrieve.size(), contactId);
        }
        Collections.sort(contactsToRetrieve);
        return contactsToRetrieve;
    }

}

