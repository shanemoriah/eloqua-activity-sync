eloqua-activity-sync
====================

Repo with test code for syncing eloqua activities via REST v1 and BULK v2

Clone from github and build with "mvn clean package" or download the compiled jar here 

Then execute with:
java -cp eloqua-activities-0.1.jar io.ntropy.EloquaActivities -eloquaCompanyName {comp name} -eloquaUsername {username} -eloquaPwd {password} -activitiesSegmentURI {activity segment uri}
