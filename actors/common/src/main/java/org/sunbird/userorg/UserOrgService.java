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
import java.util.Map;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.sunbird.common.exception.ProjectCommonException.throwServerErrorException;
import static org.sunbird.common.models.util.JsonKey.*;
import static org.sunbird.common.models.util.ProjectLogger.log;
import static org.sunbird.common.models.util.ProjectUtil.getConfigValue;
import static org.sunbird.common.responsecode.ResponseCode.errorProcessingRequest;

public class UserOrgService implements UserOrg {

    private static Map<String, String> getHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put(AUTHORIZATION, "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJlMDRkNzJkMWNiZDg0MTEyOTBkNGFiZWM3NDU5YTFlYiJ9.bThu42m1nPTMikbYGywqBqQYUihm_l1HsmKMREMuSdM");
        headers.put("Content-Type", "application/json");
        return headers;
    }

    @Override
    public Response getOragnisationDetails(String orgID)
    {
        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> request = new HashMap<>();
        requestMap.put(JsonKey.REQUEST, request);
        Map<String, String> filterlist = new HashMap<>();
        request.put(FILTERS, filterlist);
        filterlist.put("id",orgID);
        Map<String, String> headers = getHeaders();
        ObjectMapper mapper = new ObjectMapper();

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

}
