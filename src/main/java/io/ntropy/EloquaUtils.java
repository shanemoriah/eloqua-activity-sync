package io.ntropy;

import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EloquaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(EloquaUtils.class);


    public static Map<String,String> getHeaderProperties(String eloquaCompanyName, String eloquaUsername, String eloquaPwd) {
        //create required headers for authentication to API
        String plainToken = String.format("%s\\%s:%s", eloquaCompanyName, eloquaUsername, eloquaPwd);
        String base64Token = "Basic " + new String(Base64.encodeBase64(plainToken.getBytes()));
        Map<String,String> headerProperties = Maps.newHashMap();
        headerProperties.put("Authorization", base64Token);
        headerProperties.put("Content-Type", "application/json");
        headerProperties.put("Accept", "application/json");
        return headerProperties;
    }

    public static String getBaseEndpoint(Map<String,String> headerProperties) throws Exception {
        String loginURL = "https://login.eloqua.com/id";
        String loginResult = WebUtilities.sendGetRequest(loginURL, headerProperties);
        LOG.debug("{}", loginResult);
        JSONObject loginResults = new JSONObject(loginResult);
        String baseEndpoint = loginResults.getJSONObject("urls").getString("base");
        return baseEndpoint;
    }

    public static String getRESTEndpoint(String baseEndpoint, int version) {
        return String.format("%s/API/REST/%s.0", baseEndpoint, version);
    }


    public static String getBulkEndpoint(String baseEndpoint, int version) {
        return String.format("%s/API/Bulk/%s.0", baseEndpoint, version);
    }
}
