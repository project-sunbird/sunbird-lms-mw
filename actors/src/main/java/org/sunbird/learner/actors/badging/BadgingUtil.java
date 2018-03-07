package org.sunbird.learner.actors.badging;

import org.sunbird.common.models.util.ProjectUtil;

import java.util.HashMap;
import java.util.Map;

public class BadgingUtil {
    public static final String SUNBIRD_BADGR_SERVER_URL = "sunbird_badgr_server_url";
    public static final String SUNBIRD_BADGR_SERVER_URL_DEFAULT = "http://localhost:8000";

    public static final String BADGING_AUTHORIZATION_KEY = "badging_authorization_key";
    public static final String BADGING_AUTHORIZATION_FORMAT = "Token %s";


    public static String getBadgrBaseUrl() {
        if (!ProjectUtil.isStringNullOREmpty(System.getenv(SUNBIRD_BADGR_SERVER_URL))) {
            return System.getenv(SUNBIRD_BADGR_SERVER_URL);
        }
        return SUNBIRD_BADGR_SERVER_URL_DEFAULT;
    }

    public static String getBadgeClassUrl(String issuerSlug) {
        return String.format("%s/v1/issuer/issuers/%s/badges", getBadgrBaseUrl(), issuerSlug);
    }

    public static Map<String, String> getBadgrHeaders() {
        HashMap<String, String> headers = new HashMap<>();
        String authToken = System.getenv(BADGING_AUTHORIZATION_KEY);
        if (!ProjectUtil.isStringNullOREmpty(authToken)) {
            headers.put("Authorization", String.format(BADGING_AUTHORIZATION_FORMAT, authToken));
        }
        return headers;
    }
}
