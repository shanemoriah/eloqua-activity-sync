eloqua-activity-sync
====================

Repo with test code for syncing eloqua activities via REST v1 and BULK v2.  By default the code will sync activities for the past 24 hours, either using the bulk API with timerange or by exporting all contacts in a preconfigured segment (with inclusion criteria defined below) and using the REST API to query for each activity type over the 24 hour timerange. 

All output is to console.  At the end the count of activities found via the individual API (REST) will be shown adjacent to the count of activities found using the BULK 2.0 API. 

Clone from github and build with "mvn clean package" or download the compiled jar here 

Then execute with:
java -cp eloqua-activities-0.1.jar io.ntropy.EloquaActivities -eloquaCompanyName {comp name} -eloquaUsername {username} -eloquaPwd {password} -activitiesSegmentURI {activity segment uri} [-activitiesForAllContacts]

By adding the optional activitiesForAllContacts flag, the REST calls will be made for all contacts all activities in the database, this will be guaranteed most accurate but will take a long time on larger contact databases. We generally execute using a preconfigured Segment defined as users having done any of the following activities in the previous 2 days:

* EmailSentCriterion
* EmailOpenCriterion
* EmailClickThroughCriterion
* SubscriptionCriterion
* SoftBouncebackCriterion
* HardBouncebackCriterion
* VisitedLandingPageCriterion
* VisitedWebsiteCriterion
* CampaignResponderCriterion
* UnsubscriptionCriterion

 
