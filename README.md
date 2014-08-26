eloqua-activity-sync
====================

Repo with test code for syncing eloqua activities via REST v1 and BULK v2

Clone from github and build with "mvn clean package"

Then execute with:
java -cp target/eloqua-activities-0.1.jar io.ntropy.EloquaActivities -eloquaCompanyName {comp name} -eloquaUsername {username} -eloquaPwd {password} -activitiesSegmentURI {activity segment uri}
