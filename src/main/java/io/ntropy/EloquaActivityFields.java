package io.ntropy;

import com.google.common.collect.Maps;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;

public class EloquaActivityFields {

    public static Map<String, JSONObject> getActivityFields() throws JSONException {
        Map<String, JSONObject> activityTypeToFields = Maps.newHashMap();
        activityTypeToFields.put("EmailOpen", getEmailOpenFields());
        activityTypeToFields.put("EmailClickthrough", getEmailClickthroughFields());
        activityTypeToFields.put("EmailSend", getEmailSendFields());
        activityTypeToFields.put("Subscribe", getSubscribeFields());
        activityTypeToFields.put("Unsubscribe", getUnsubscribeFields());
        activityTypeToFields.put("Bounceback", getBouncebackFields());
        activityTypeToFields.put("WebVisit", getWebVisitFields());
        activityTypeToFields.put("PageView", getPageViewFields());
        activityTypeToFields.put("FormSubmit", getFormSubmitFields());
        return activityTypeToFields;
    }

    private static JSONObject getEmailOpenFields() throws JSONException {
        JSONObject emailOpenFields = new JSONObject();
        emailOpenFields.put("ActivityId","{{Activity.Id}}");
        emailOpenFields.put("ActivityType","{{Activity.Type}}");
        emailOpenFields.put("ActivityDate","{{Activity.CreatedAt}}");
        emailOpenFields.put("EmailAddress","{{Activity.Field(EmailAddress)}}");
        emailOpenFields.put("ContactId","{{Activity.Contact.Id}}");
        emailOpenFields.put("IpAddress","{{Activity.Field(IpAddress)}}");
        emailOpenFields.put("VisitorId","{{Activity.Visitor.Id}}");
        emailOpenFields.put("EmailRecipientId","{{Activity.Field(EmailRecipientId)}}");
        emailOpenFields.put("AssetType","{{Activity.Asset.Type}}");
        emailOpenFields.put("AssetName","{{Activity.Asset.Name}}");
        emailOpenFields.put("AssetId","{{Activity.Asset.Id}}");
        emailOpenFields.put("SubjectLine","{{Activity.Field(SubjectLine)}}");
        emailOpenFields.put("EmailWebLink","{{Activity.Field(EmailWebLink)}}");
        emailOpenFields.put("VisitorExternalId","{{Activity.Visitor.ExternalId}}");
              emailOpenFields.put("CampaignId","{{Activity.Campaign.Id}}");
        return emailOpenFields;
    }

    private static JSONObject getEmailClickthroughFields() throws JSONException {
        JSONObject emailClickthroughFields = new JSONObject();
        emailClickthroughFields.put("ActivityId","{{Activity.Id}}");
        emailClickthroughFields.put("ActivityType","{{Activity.Type}}");
        emailClickthroughFields.put("ActivityDate","{{Activity.CreatedAt}}");
        emailClickthroughFields.put("EmailAddress","{{Activity.Field(EmailAddress)}}");
        emailClickthroughFields.put("ContactId","{{Activity.Contact.Id}}");
        emailClickthroughFields.put("IpAddress","{{Activity.Field(IpAddress)}}");
        emailClickthroughFields.put("VisitorId","{{Activity.Visitor.Id}}");
        emailClickthroughFields.put("EmailRecipientId","{{Activity.Field(EmailRecipientId)}}");
        emailClickthroughFields.put("AssetType","{{Activity.Asset.Type}}");
        emailClickthroughFields.put("AssetName","{{Activity.Asset.Name}}");
        emailClickthroughFields.put("AssetId","{{Activity.Asset.Id}}");
        emailClickthroughFields.put("SubjectLine","{{Activity.Field(SubjectLine)}}");
        emailClickthroughFields.put("EmailWebLink","{{Activity.Field(EmailWebLink)}}");
                emailClickthroughFields.put("CampaignId","{{Activity.Campaign.Id}}");
                emailClickthroughFields.put("EmailClickedThruLink","{{Activity.Field(EmailClickedThruLink)}}");
        return emailClickthroughFields;
    }

    private static JSONObject getEmailSendFields() throws JSONException {
        JSONObject emailSendFields = new JSONObject();
        emailSendFields.put("ActivityId","{{Activity.Id}}");
        emailSendFields.put("ActivityType","{{Activity.Type}}");
        emailSendFields.put("ActivityDate","{{Activity.CreatedAt}}");
        emailSendFields.put("EmailAddress","{{Activity.Field(EmailAddress)}}");
        emailSendFields.put("EmailRecipientId","{{Activity.Field(EmailRecipientId)}}");
        emailSendFields.put("AssetType","{{Activity.Asset.Type}}");
        emailSendFields.put("AssetName","{{Activity.Asset.Name}}");
        emailSendFields.put("AssetId","{{Activity.Asset.Id}}");
        emailSendFields.put("SubjectLine","{{Activity.Field(SubjectLine)}}");
        emailSendFields.put("EmailWebLink","{{Activity.Field(EmailWebLink)}}");
                emailSendFields.put("CampaignId","{{Activity.Campaign.Id}}");
        return emailSendFields;
    }

    private static JSONObject getSubscribeFields() throws JSONException {
        JSONObject subscribeFields = new JSONObject();
        subscribeFields.put("ActivityId","{{Activity.Id}}");
        subscribeFields.put("ActivityType","{{Activity.Type}}");
        subscribeFields.put("AssetId","{{Activity.Asset.Id}}");
        subscribeFields.put("ActivityDate","{{Activity.CreatedAt}}");
        subscribeFields.put("EmailAddress","{{Activity.Field(EmailAddress)}}");
        subscribeFields.put("EmailRecipientId","{{Activity.Field(EmailRecipientId)}}");
        subscribeFields.put("AssetType","{{Activity.Asset.Type}}");
        subscribeFields.put("AssetName","{{Activity.Asset.Name}}");
                subscribeFields.put("CampaignId","{{Activity.Campaign.Id}}");
        return subscribeFields;
    }

    private static JSONObject getUnsubscribeFields() throws JSONException {
        JSONObject unsubscribeFields = new JSONObject();
        unsubscribeFields.put("ActivityId","{{Activity.Id}}");
        unsubscribeFields.put("ActivityType","{{Activity.Type}}");
        unsubscribeFields.put("AssetId","{{Activity.Asset.Id}}");
        unsubscribeFields.put("ActivityDate","{{Activity.CreatedAt}}");
        unsubscribeFields.put("EmailAddress","{{Activity.Field(EmailAddress)}}");
        unsubscribeFields.put("EmailRecipientId","{{Activity.Field(EmailRecipientId)}}");
        unsubscribeFields.put("AssetType","{{Activity.Asset.Type}}");
        unsubscribeFields.put("AssetName","{{Activity.Asset.Name}}");
                unsubscribeFields.put("CampaignId","{{Activity.Campaign.Id}}");
        return unsubscribeFields;
    }

    private static JSONObject getBouncebackFields() throws JSONException {
        JSONObject bouncebackFields = new JSONObject();
        bouncebackFields.put("ActivityId","{{Activity.Id}}");
        bouncebackFields.put("ActivityType","{{Activity.Type}}");
        bouncebackFields.put("ActivityDate","{{Activity.CreatedAt}}");
        bouncebackFields.put("EmailAddress","{{Activity.Field(EmailAddress)}}");
        return bouncebackFields;
    }

    private static JSONObject getWebVisitFields() throws JSONException {
        JSONObject webVisitFields = new JSONObject();
        webVisitFields.put("ActivityId","{{Activity.Id}}");
        webVisitFields.put("ActivityType","{{Activity.Type}}");
        webVisitFields.put("ActivityDate","{{Activity.CreatedAt}}");
        webVisitFields.put("ContactId","{{Activity.Contact.Id}}");
        webVisitFields.put("VisitorId","{{Activity.Visitor.Id}}");
        webVisitFields.put("Duration","{{Activity.Field(Duration)}}");
        webVisitFields.put("IpAddress","{{Activity.Field(IpAddress)}}");
        webVisitFields.put("VisitorExternalId","{{Activity.Visitor.ExternalId}}");
                webVisitFields.put("ReferrerUrl","{{Activity.Field(ReferrerUrl)}}");
                webVisitFields.put("NumberOfPages","{{Activity.Field(NumberOfPages)}}");
                webVisitFields.put("FirstPageViewUrl","{{Activity.Field(FirstPageViewUrl)}}");
        return webVisitFields;
    }

    private static JSONObject getPageViewFields() throws JSONException {
        JSONObject pageViewFields = new JSONObject();
        pageViewFields.put("ActivityId","{{Activity.Id}}");
        pageViewFields.put("ActivityType","{{Activity.Type}}");
        pageViewFields.put("ActivityDate","{{Activity.CreatedAt}}");
        pageViewFields.put("ContactId","{{Activity.Contact.Id}}");
        pageViewFields.put("VisitorId","{{Activity.Visitor.Id}}");
        pageViewFields.put("VisitorExternalId","{{Activity.Visitor.ExternalId}}");
        pageViewFields.put("WebVisitId","{{Activity.Field(WebVisitId)}}");
        pageViewFields.put("IpAddress","{{Activity.Field(IpAddress)}}");
                pageViewFields.put("IsWebTrackingOptedIn","{{Activity.Field(IsWebTrackingOptedIn)}}");
                pageViewFields.put("CampaignId","{{Activity.Campaign.Id}}");
                pageViewFields.put("Url","{{Activity.Field(Url)}}");
                pageViewFields.put("ReferrerUrl","{{Activity.Field(ReferrerUrl)}}");

        return pageViewFields;
    }

    private static JSONObject getFormSubmitFields() throws JSONException {
        JSONObject formSubmitFields = new JSONObject();
        formSubmitFields.put("ActivityId","{{Activity.Id}}");
        formSubmitFields.put("ActivityType","{{Activity.Type}}");
        formSubmitFields.put("ActivityDate","{{Activity.CreatedAt}}");
        formSubmitFields.put("ContactId","{{Activity.Contact.Id}}");
        formSubmitFields.put("VisitorId","{{Activity.Visitor.Id}}");
        formSubmitFields.put("VisitorExternalId","{{Activity.Visitor.ExternalId}}");
        formSubmitFields.put("AssetType","{{Activity.Asset.Type}}");
        formSubmitFields.put("AssetId","{{Activity.Asset.Id}}");
        formSubmitFields.put("AssetName","{{Activity.Asset.Name}}");
                formSubmitFields.put("RawData","{{Activity.Field(RawData)}}");
        return formSubmitFields;
    }



}
