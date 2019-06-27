package org.sunbird.userorg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.responsecode.ResponseCode;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.sunbird.common.exception.ProjectCommonException.throwServerErrorException;
import static org.sunbird.common.models.util.JsonKey.*;
import static org.sunbird.common.models.util.ProjectLogger.log;
import static org.sunbird.common.models.util.ProjectUtil.getConfigValue;
import static org.sunbird.common.responsecode.ResponseCode.errorProcessingRequest;

public class UserOrgService implements UserOrg {

    private static Map<String, String> getdefaultHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put(AUTHORIZATION, "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJlMDRkNzJkMWNiZDg0MTEyOTBkNGFiZWM3NDU5YTFlYiJ9.bThu42m1nPTMikbYGywqBqQYUihm_l1HsmKMREMuSdM");
        headers.put("Content-Type", "application/json");
        return headers;
    }

    @Override
    public Response getOrganisationDetails(String orgID)
    {
        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> request = new HashMap<>();
        requestMap.put(JsonKey.REQUEST, request);
        Map<String, String> filterlist = new HashMap<>();
        request.put(FILTERS, filterlist);
        filterlist.put("id",orgID);
        Map<String, String> headers = getdefaultHeaders();
        ObjectMapper mapper=new ObjectMapper();

        Response response = null;
        try {
            String requestUrl =
                    "https://dev.sunbirded.org/api"
                            + getConfigValue(SUNBIRD_GET_ORGANISATION_API);
            String reqBody = mapper.writeValueAsString(requestMap);

            HttpResponse<String> httpResponse = Unirest.post(requestUrl).headers(headers).body(reqBody).asString();

            if (StringUtils.isBlank(httpResponse.getBody())) {
                throwServerErrorException(
                        ResponseCode.SERVER_ERROR, errorProcessingRequest.getErrorMessage());
            }
            response = mapper.readValue(httpResponse.getBody(), Response.class);
            if (!ResponseCode.OK.equals(response.getResponseCode())) {

                throw new ProjectCommonException(
                        response.getResponseCode().name(),
                        response.getParams().getErrmsg(),
                        response.getResponseCode().getResponseCode());
            }
        } catch (IOException e) {
            log(
                    "Exception occurred with error message = "
                            + e.getMessage(),
                    e);
            throwServerErrorException(ResponseCode.SERVER_ERROR);
        } catch (UnirestException e) {
            log(
                    "Exception occurred with error message = "
                            + e.getMessage(),
                    e);
            throwServerErrorException(ResponseCode.SERVER_ERROR);
        }
        return response;
    }

    @Override
    public Response getOrganisationDetailsForMultipleOrgIds(List<String> orgfields)
    {
        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> request = new HashMap<>();
        requestMap.put(JsonKey.REQUEST, request);
        Map<String, List<String>> filterlist = new HashMap<>();
        request.put(FILTERS, filterlist);
        filterlist.put("id",orgfields);
        Map<String, String> headers = getdefaultHeaders();
        ObjectMapper mapper=new ObjectMapper();

        Response response = null;
        try {
            String requestUrl =
                    "https://dev.sunbirded.org/api"
                            + getConfigValue(SUNBIRD_GET_ORGANISATION_API);
            String reqBody = mapper.writeValueAsString(requestMap);

            HttpResponse<String> httpResponse = Unirest.post(requestUrl).headers(headers).body(reqBody).asString();

            if (StringUtils.isBlank(httpResponse.getBody())) {
                throwServerErrorException(
                        ResponseCode.SERVER_ERROR, errorProcessingRequest.getErrorMessage());
            }
            response = mapper.readValue(httpResponse.getBody(), Response.class);
            if (!ResponseCode.OK.equals(response.getResponseCode())) {

                throw new ProjectCommonException(
                        response.getResponseCode().name(),
                        response.getParams().getErrmsg(),
                        response.getResponseCode().getResponseCode());
            }
        } catch (IOException e) {
            log(
                    "Exception occurred with error message = "
                            + e.getMessage(),
                    e);
            throwServerErrorException(ResponseCode.SERVER_ERROR);
        } catch (UnirestException e) {
            log(
                    "Exception occurred with error message = "
                            + e.getMessage(),
                    e);
            throwServerErrorException(ResponseCode.SERVER_ERROR);
        }
        return response;
    }
    @Override
    public Response getUserDetailsForSingleUserID(String userID)
    {
        Map<String, String> headers = getdefaultHeaders();
        headers.put("x-authenticated-user-token","eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ1WXhXdE4tZzRfMld5MG5PS1ZoaE5hU0gtM2lSSjdXU25ibFlwVVU0TFRrIn0.eyJqdGkiOiJkODljNzdmMy1lYmRjLTQ1YmQtYTc1MS0zYTVmMWI0MjgwM2UiLCJleHAiOjE1NjE1NTY1NTQsIm5iZiI6MCwiaWF0IjoxNTYxNTM4NTU0LCJpc3MiOiJodHRwczovL2Rldi5zdW5iaXJkZWQub3JnL2F1dGgvcmVhbG1zL3N1bmJpcmQiLCJhdWQiOiJhZG1pbi1jbGkiLCJzdWIiOiJmOjVhOGEzZjJiLTM0MDktNDJlMC05MDAxLWY5MTNiYzBmZGUzMTo4NzRlZDhhNS03ODJlLTRmNmMtOGYzNi1lMDI4ODQ1NTkwMWUiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJhZG1pbi1jbGkiLCJhdXRoX3RpbWUiOjAsInNlc3Npb25fc3RhdGUiOiI3MGRkMzMwNi0zZTY1LTQ3ODEtYWQyNC02NWU5Y2JhOWJlMTgiLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnt9LCJuYW1lIjoiQ3JlYXRpb24iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJudHB0ZXN0MTAyIiwiZ2l2ZW5fbmFtZSI6IkNyZWF0aW9uIiwiZmFtaWx5X25hbWUiOiIiLCJlbWFpbCI6IjEyMzQ1NkBnbWFpbC5jbyJ9.cT4GITQ-371rP-YoZvQecJgjv2EZkGQY9cLu6Rb4fRhONdEu2QWj-Utxl5UbhVsEsQudGQoJXcOLuioIRZZIH2gqoJHJaCA9V5vCqilp8jQ5co1MJVaNEcPrOUNMjAwKYXLctH_pAiLuCcbYeKo_d_jQRD9IyJzs3AInBpWer_UiQbyKy7KRKHJsogCGCvG6_yADWQ31y7sCzGfMCXTHPSauYarexVoWgSMyGyJDqjKWmgxSHdzo_uXXWacOxC2zw8aHZuQgIHHlb5061xrQKKtScFQ7g_NrgNUQtyzOqGTVZJ3K0QQSCFZRKWwxDd7Ggk2chJl_-RxPTcdZvZTGdQ");

        ObjectMapper mapper=new ObjectMapper();
        userID="874ed8a5-782e-4f6c-8f36-e0288455901e";
        Response response = null;
        try {
            String requestUrl =
                    "https://dev.sunbirded.org/learner"
                            + getConfigValue(SUNBIRD_GET_SINGLE_USER_API)+"/"+userID;

            HttpResponse<String> httpResponse = Unirest.get(requestUrl).headers(headers).asString();

            if (StringUtils.isBlank(httpResponse.getBody())) {
                throwServerErrorException(
                        ResponseCode.SERVER_ERROR, errorProcessingRequest.getErrorMessage());
            }
            response = mapper.readValue(httpResponse.getBody(), Response.class);
            if (!ResponseCode.OK.equals(response.getResponseCode())) {

                throw new ProjectCommonException(
                        response.getResponseCode().name(),
                        response.getParams().getErrmsg(),
                        response.getResponseCode().getResponseCode());
            }
        } catch (IOException e) {
            log(
                    "Exception occurred with error message = "
                            + e.getMessage(),
                    e);
            throwServerErrorException(ResponseCode.SERVER_ERROR);
        } catch (UnirestException e) {
            log(
                    "Exception occurred with error message = "
                            + e.getMessage(),
                    e);
            throwServerErrorException(ResponseCode.SERVER_ERROR);
        }
        return response;
    }

    @Override
    public Response getUserDetailsForMultipleUserIDs(List<String> userIds)
    {
        userIds.add("874ed8a5-782e-4f6c-8f36-e0288455901e");
        userIds.add("95e4942d-cbe8-477d-aebd-ad8e6de4bfc8");
        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> request = new HashMap<>();
        requestMap.put(JsonKey.REQUEST, request);
        Map<String, List<String>> filterList = new HashMap<>();
        request.put(FILTERS, filterList);
        filterList.put("id",userIds);
        Map<String, String> headers = getdefaultHeaders();
        headers.put("x-authenticated-user-token","eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ1WXhXdE4tZzRfMld5MG5PS1ZoaE5hU0gtM2lSSjdXU25ibFlwVVU0TFRrIn0.eyJqdGkiOiJkODljNzdmMy1lYmRjLTQ1YmQtYTc1MS0zYTVmMWI0MjgwM2UiLCJleHAiOjE1NjE1NTY1NTQsIm5iZiI6MCwiaWF0IjoxNTYxNTM4NTU0LCJpc3MiOiJodHRwczovL2Rldi5zdW5iaXJkZWQub3JnL2F1dGgvcmVhbG1zL3N1bmJpcmQiLCJhdWQiOiJhZG1pbi1jbGkiLCJzdWIiOiJmOjVhOGEzZjJiLTM0MDktNDJlMC05MDAxLWY5MTNiYzBmZGUzMTo4NzRlZDhhNS03ODJlLTRmNmMtOGYzNi1lMDI4ODQ1NTkwMWUiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJhZG1pbi1jbGkiLCJhdXRoX3RpbWUiOjAsInNlc3Npb25fc3RhdGUiOiI3MGRkMzMwNi0zZTY1LTQ3ODEtYWQyNC02NWU5Y2JhOWJlMTgiLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnt9LCJuYW1lIjoiQ3JlYXRpb24iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJudHB0ZXN0MTAyIiwiZ2l2ZW5fbmFtZSI6IkNyZWF0aW9uIiwiZmFtaWx5X25hbWUiOiIiLCJlbWFpbCI6IjEyMzQ1NkBnbWFpbC5jbyJ9.cT4GITQ-371rP-YoZvQecJgjv2EZkGQY9cLu6Rb4fRhONdEu2QWj-Utxl5UbhVsEsQudGQoJXcOLuioIRZZIH2gqoJHJaCA9V5vCqilp8jQ5co1MJVaNEcPrOUNMjAwKYXLctH_pAiLuCcbYeKo_d_jQRD9IyJzs3AInBpWer_UiQbyKy7KRKHJsogCGCvG6_yADWQ31y7sCzGfMCXTHPSauYarexVoWgSMyGyJDqjKWmgxSHdzo_uXXWacOxC2zw8aHZuQgIHHlb5061xrQKKtScFQ7g_NrgNUQtyzOqGTVZJ3K0QQSCFZRKWwxDd7Ggk2chJl_-RxPTcdZvZTGdQ");

        ObjectMapper mapper=new ObjectMapper();

        Response response = null;
        try {
            String requestUrl =
                    "https://dev.sunbirded.org/api"
                            + getConfigValue(SUNBIRD_GET_MULTIPLE_USER_API);
            String reqBody = mapper.writeValueAsString(requestMap);

            HttpResponse<String> httpResponse = Unirest.post(requestUrl).headers(headers).body(reqBody).asString();

            if (StringUtils.isBlank(httpResponse.getBody())) {
                throwServerErrorException(
                        ResponseCode.SERVER_ERROR, errorProcessingRequest.getErrorMessage());
            }
            response = mapper.readValue(httpResponse.getBody(), Response.class);
            if (!ResponseCode.OK.equals(response.getResponseCode())) {

                throw new ProjectCommonException(
                        response.getResponseCode().name(),
                        response.getParams().getErrmsg(),
                        response.getResponseCode().getResponseCode());
            }
        } catch (IOException e) {
            log(
                    "Exception occurred with error message = "
                            + e.getMessage(),
                    e);
            throwServerErrorException(ResponseCode.SERVER_ERROR);
        } catch (UnirestException e) {
            log(
                    "Exception occurred with error message = "
                            + e.getMessage(),
                    e);
            throwServerErrorException(ResponseCode.SERVER_ERROR);
        }
        return response;
    }

}
