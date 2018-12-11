package org.sunbird.user.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.apache.http.HttpHeaders;
import org.sunbird.common.models.response.HttpUtilResponse;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.PropertiesCache;

public class GoogleAuthCodeVerificationService {

  private GoogleAuthCodeVerificationService() {}

  @SuppressWarnings("unchecked")
  public static Map<String, Object> verifyCode(String verificationCode) {
    String urlString =
        MessageFormat.format(
            PropertiesCache.getInstance().readProperty(JsonKey.GOOGLE_VERIFY_AUTH_TOKEN_URL),
            verificationCode);
    Map<String, String> headers = new HashMap<>();
    headers.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    headers.put(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
    try {
      HttpUtilResponse response = HttpUtil.doGetRequest(urlString, headers);
      ObjectMapper mapper = new ObjectMapper();
      return mapper.readValue(response.getBody(), Map.class);
    } catch (Exception e) {
      ProjectLogger.log("Exception occurred while verifying google auth token", e);
    }
    return null;
  }
}
