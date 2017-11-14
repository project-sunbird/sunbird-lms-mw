package org.sunbird.metrics.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class OrganisationMetricsUtil {

  public static List<String> operationList = new ArrayList<>();
  private static ObjectMapper mapper = new ObjectMapper();

  protected enum ContentStatus {
    Draft("Create"), Review("Review"), Live("Publish");

    private String contentOperation;

    private ContentStatus(String operation) {
      this.contentOperation = operation;
    }

    private String getOperation() {
      return this.contentOperation;
    }

  }

  static {
    operationList.add(ContentStatus.Draft.getOperation());
    operationList.add(ContentStatus.Review.getOperation());
    operationList.add(ContentStatus.Live.getOperation());
  }

  public static Map<String, Object> validateOrg(String orgId) {
    try {
      Map<String, Object> result =
          ElasticSearchUtil.getDataByIdentifier(ProjectUtil.EsIndex.sunbird.getIndexName(),
              ProjectUtil.EsType.organisation.getTypeName(), orgId);
      if (null == result || result.isEmpty()) {
        return null;
      }
      ProjectLogger.log("Result:" + result.toString());
      return result;
    } catch (Exception e) {
      ProjectLogger.log("Error occured", e);
      throw new ProjectCommonException(ResponseCode.esError.getErrorCode(),
          ResponseCode.esError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  public static String getOrgMetricsRequest(Request actorMessage, String periodStr, String orgHashId,
      String userId, String channel) throws JsonProcessingException {
    Request request = new Request();
    request.setId(actorMessage.getId());
    Map<String, Object> requestObject = new HashMap<>();
    requestObject.put(JsonKey.PERIOD, BaseMetricsActor.getEkstepPeriod(periodStr));
    Map<String, Object> filterMap = new HashMap<>();
    filterMap.put(JsonKey.TAG, orgHashId);
    if (!ProjectUtil.isStringNullOREmpty(userId)) {
      filterMap.put(BaseMetricsActor.USER_ID, userId);
    }
    requestObject.put(JsonKey.FILTER, filterMap);
    requestObject.put(JsonKey.CHANNEL, channel);
    request.setRequest(requestObject);
    String requestStr = mapper.writeValueAsString(request);
    return requestStr;
  }
  
}