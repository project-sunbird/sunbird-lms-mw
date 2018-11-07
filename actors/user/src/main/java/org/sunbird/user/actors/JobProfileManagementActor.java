package org.sunbird.user.actors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.user.dao.AddressDao;
import org.sunbird.user.dao.JobProfileDao;
import org.sunbird.user.dao.impl.AddressDaoImpl;
import org.sunbird.user.dao.impl.JobProfileDaoImpl;

@ActorConfig(
  tasks = {"insertUserJobProfile", "updateUserJobProfile"},
  asyncTasks = {"insertUserJobProfile", "updateUserJobProfile"}
)
public class JobProfileManagementActor extends BaseActor {

  private AddressDao addressDao = AddressDaoImpl.getInstance();
  private JobProfileDao jobProfileDao = JobProfileDaoImpl.getInstance();

  @Override
  public void onReceive(Request request) throws Throwable {
    String operation = request.getOperation();
    switch (operation) {
      case "insertUserJobProfile":
        insertJobProfile(request);
        break;

      case "updateUserJobProfile":
        updateJobProfile(request);
        break;

      default:
        onReceiveUnsupportedOperation("JobProfileManagementActor");
    }
  }

  @SuppressWarnings("unchecked")
  private void insertJobProfile(Request request) {
    Map<String, Object> requestMap = request.getRequest();
    List<Map<String, Object>> reqList =
        (List<Map<String, Object>>) requestMap.get(JsonKey.JOB_PROFILE);
    Response response = new Response();
    List<String> errMsgs = new ArrayList<>();
    try {
      for (int i = 0; i < reqList.size(); i++) {
        Map<String, Object> jobProfileMap = reqList.get(i);
        String createdBy = (String) requestMap.get(JsonKey.CREATED_BY);
        Response addrResponse = null;
        jobProfileMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i));
        if (jobProfileMap.containsKey(JsonKey.ADDRESS)) {
          addrResponse = upsertJobProfileAddressDetails(jobProfileMap, createdBy);
        }
        insertJobProfileDetails(requestMap, jobProfileMap, addrResponse, createdBy);
      }
    } catch (Exception e) {
      errMsgs.add(e.getMessage());
      ProjectLogger.log(e.getMessage(), e);
    }
    if (CollectionUtils.isNotEmpty(errMsgs)) {
      response.put(JsonKey.JOB_PROFILE + ":" + JsonKey.ERROR_MSG, errMsgs);
    } else {
      response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    }
    sender().tell(response, self());
  }

  @SuppressWarnings("unchecked")
  private void updateJobProfile(Request request) {
    Map<String, Object> requestMap = request.getRequest();
    List<Map<String, Object>> reqList =
        (List<Map<String, Object>>) requestMap.get(JsonKey.JOB_PROFILE);
    Response response = new Response();
    List<String> errMsgs = new ArrayList<>();
    try {
      for (int i = 0; i < reqList.size(); i++) {
        Map<String, Object> jobProfileMap = reqList.get(i);
        String createdBy = (String) requestMap.get(JsonKey.CREATED_BY);
        Response addrResponse = null;
        if (BooleanUtils.isTrue((boolean) jobProfileMap.get(JsonKey.IS_DELETED))
            && !StringUtils.isBlank((String) jobProfileMap.get(JsonKey.ID))) {
          deleteJobProfileDetails(jobProfileMap);
          continue;
        }
        if (jobProfileMap.containsKey(JsonKey.ADDRESS)) {
          addrResponse = upsertJobProfileAddressDetails(jobProfileMap, createdBy);
        }
        if (StringUtils.isBlank((String) jobProfileMap.get(JsonKey.ID))) {
          jobProfileMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(i));
          insertJobProfileDetails(requestMap, jobProfileMap, addrResponse, createdBy);
        } else {
          updateJobProfileDetails(jobProfileMap, addrResponse, createdBy);
        }
      }
    } catch (Exception e) {
      errMsgs.add(e.getMessage());
      ProjectLogger.log(e.getMessage(), e);
    }
    if (CollectionUtils.isNotEmpty(errMsgs)) {
      response.put(JsonKey.JOB_PROFILE + ":" + JsonKey.ERROR_MSG, errMsgs);
    } else {
      response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    }
    sender().tell(response, self());
  }

  private void updateJobProfileDetails(
      Map<String, Object> jobProfileMap, Response addrResponse, String createdBy) {
    if (null != addrResponse
        && ((String) addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      jobProfileMap.put(JsonKey.ADDRESS_ID, addrResponse.get(JsonKey.ADDRESS_ID));
      jobProfileMap.remove(JsonKey.ADDRESS);
    }
    jobProfileMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    jobProfileMap.put(JsonKey.UPDATED_BY, createdBy);
    jobProfileMap.remove(JsonKey.USER_ID);
    jobProfileDao.upsertJobProfile(jobProfileMap);
  }

  @SuppressWarnings("unchecked")
  private void deleteJobProfileDetails(Map<String, Object> requestMap) {
    String addrsId = null;
    if (requestMap.containsKey(JsonKey.ADDRESS) && null != requestMap.get(JsonKey.ADDRESS)) {
      addrsId = (String) ((Map<String, Object>) requestMap.get(JsonKey.ADDRESS)).get(JsonKey.ID);
    } else {
      addrsId = getAddressId((String) requestMap.get(JsonKey.ID));
    }
    if (null != addrsId) {
      addressDao.deleteAddress(addrsId);
    }
    jobProfileDao.deleteJobProfile((String) requestMap.get(JsonKey.ID));
  }

  @SuppressWarnings("unchecked")
  private String getAddressId(String id) {
    String addressId = null;
    try {
      Response res = jobProfileDao.getPropertiesValueById(JsonKey.ADDRESS_ID, id);
      if (!((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).isEmpty()) {
        addressId =
            (String)
                (((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).get(0))
                    .get(JsonKey.ADDRESS_ID);
      }
    } catch (Exception ex) {
      ProjectLogger.log(ex.getMessage(), ex);
    }
    return addressId;
  }

  @SuppressWarnings("unchecked")
  private void insertJobProfileDetails(
      Map<String, Object> requestMap,
      Map<String, Object> jobProfileMap,
      Response addrResponse,
      String createdBy) {
    Map<String, Object> address = null;
    if (null != addrResponse
        && ((String) addrResponse.get(JsonKey.RESPONSE)).equalsIgnoreCase(JsonKey.SUCCESS)) {
      jobProfileMap.put(JsonKey.ADDRESS_ID, addrResponse.get(JsonKey.ADDRESS_ID));
      address = (Map<String, Object>) jobProfileMap.get(JsonKey.ADDRESS);
      jobProfileMap.remove(JsonKey.ADDRESS);
    }
    jobProfileMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    jobProfileMap.put(JsonKey.CREATED_BY, createdBy);
    jobProfileMap.put(JsonKey.USER_ID, requestMap.get(JsonKey.ID));
    jobProfileDao.createJobProfile(jobProfileMap);
    jobProfileMap.put(JsonKey.ADDRESS, address);
  }

  @SuppressWarnings("unchecked")
  private Response upsertJobProfileAddressDetails(
      Map<String, Object> jobProfileDetailsMap, String createdBy) {
    Response addrResponse = null;
    String addrId = null;
    Map<String, Object> address = (Map<String, Object>) jobProfileDetailsMap.get(JsonKey.ADDRESS);
    address.remove(JsonKey.IS_DELETED);
    if (!address.containsKey(JsonKey.ID)) {
      addrId = ProjectUtil.getUniqueIdFromTimestamp(2);
      address.put(JsonKey.ID, addrId);
      address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
      address.put(JsonKey.CREATED_BY, createdBy);
    } else {
      addrId = (String) address.get(JsonKey.ID);
      address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
      address.put(JsonKey.UPDATED_BY, createdBy);
      address.remove(JsonKey.USER_ID);
    }
    addrResponse = addressDao.upsertAddress(address);
    addrResponse.put(JsonKey.ADDRESS_ID, addrId);
    return addrResponse;
  }
}
