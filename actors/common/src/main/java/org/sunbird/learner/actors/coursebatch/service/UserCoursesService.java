package org.sunbird.learner.actors.coursebatch.service;

import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.actors.coursebatch.dao.UserCoursesDao;
import org.sunbird.learner.actors.coursebatch.dao.impl.UserCoursesDaoImpl;
import org.sunbird.models.user.courses.UserCourses;

public class UserCoursesService {
  private UserCoursesDao userCourseDao = UserCoursesDaoImpl.getInstance();

  public static void validateUserUnenroll(UserCourses userCourseResult) {
    if (userCourseResult == null) {
      ProjectLogger.log(
          "UserCoursesService:validateUserUnenroll: User is not enrolled yet",
          LoggerEnum.INFO.name());
      throw new ProjectCommonException(
          ResponseCode.userNotEnrolledCourse.getErrorCode(),
          ResponseCode.userNotEnrolledCourse.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if (!userCourseResult.isActive()) {
      ProjectLogger.log(
          "UserCoursesService:validateUserUnenroll: User does not have an enrolled course");
      throw new ProjectCommonException(
          ResponseCode.userNotEnrolledCourse.getErrorCode(),
          ResponseCode.userNotEnrolledCourse.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
    if (userCourseResult.getProgress() > 0
        && (userCourseResult.getProgress() == userCourseResult.getLeafNodesCount())) {
      ProjectLogger.log(
          "UserCoursesService:validateUserUnenroll: User already completed the course");
      throw new ProjectCommonException(
          ResponseCode.userAlreadyCompletedCourse.getErrorCode(),
          ResponseCode.userAlreadyCompletedCourse.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }
  }

  public static String getPrimaryKey(Map<String, Object> userCourseMap) {
    String userId = (String) userCourseMap.get(JsonKey.USER_ID);
    String courseId = (String) userCourseMap.get(JsonKey.COURSE_ID);
    String batchId = (String) userCourseMap.get(JsonKey.BATCH_ID);
    return getPrimaryKey(userId, courseId, batchId);
  }

  public static String getPrimaryKey(String userId, String courseId, String batchId) {
    return OneWayHashing.encryptVal(
        userId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + batchId);
  }

  public Boolean enroll(
      String batchId, String courseId, String userId, Map<String, String> additionalCourseInfo) {
    Boolean flag = false;
    Map<String, Object> userCourses = new HashMap<>();
    userCourses.put(JsonKey.USER_ID, userId);
    userCourses.put(JsonKey.BATCH_ID, batchId);
    userCourses.put(JsonKey.COURSE_ID, courseId);
    userCourses.put(JsonKey.ID, getPrimaryKey(userId, courseId, batchId));
    userCourses.put(JsonKey.CONTENT_ID, courseId);
    userCourses.put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getFormattedDate());
    userCourses.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());
    userCourses.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
    userCourses.put(JsonKey.COURSE_PROGRESS, 0);
    userCourses.put(JsonKey.COURSE_LOGO_URL, additionalCourseInfo.get(JsonKey.COURSE_LOGO_URL));
    userCourses.put(JsonKey.COURSE_NAME, additionalCourseInfo.get(JsonKey.COURSE_NAME));
    userCourses.put(JsonKey.DESCRIPTION, additionalCourseInfo.get(JsonKey.DESCRIPTION));
    if (!StringUtils.isBlank(additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT))) {
      userCourses.put(
          JsonKey.LEAF_NODE_COUNT,
          Integer.parseInt("" + additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT)));
    }
    userCourses.put(JsonKey.TOC_URL, additionalCourseInfo.get(JsonKey.TOC_URL));
    try {
      userCourseDao.insert(userCourses);
      sync(userCourses, (String) userCourses.get(JsonKey.ID));
      syncUserCourses(
          courseId,
          batchId,
          (String) userCourses.get(JsonKey.COURSE_ENROLL_DATE),
          (Integer) userCourses.get(JsonKey.COURSE_PROGRESS),
          null,
          userId);
      flag = true;
    } catch (Exception ex) {
      ProjectLogger.log(
          "UserCoursesService:enroll: Exception occurred with error message = " + ex.getMessage(),
          ex);
      flag = false;
    }
    return flag;
  }

  public void unenroll(String userId, String courseId, String batchId) {
    UserCourses userCourses = userCourseDao.read(getPrimaryKey(userId, courseId, batchId));
    validateUserUnenroll(userCourses);
    Map<String, Object> updateAttributes = new HashMap<>();
    updateAttributes.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.INACTIVE.getValue());
    updateAttributes.put(JsonKey.ID, userCourses.getId());
    userCourseDao.update(updateAttributes);
    sync(updateAttributes, userCourses.getId());
    syncRemoveUserCourses(batchId, userId);
  }

  public Map<String, Object> getActiveUserCourses(String userId) {
    Map<String, Object> filter = new HashMap<>();
    filter.put(JsonKey.USER_ID, userId);
    filter.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());
    SearchDTO searchDto = new SearchDTO();
    searchDto.getAdditionalProperties().put(JsonKey.FILTERS, filter);
    return ElasticSearchUtil.complexSearch(
        searchDto,
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.usercourses.getTypeName());
  }

  public static void sync(Map<String, Object> courseMap, String id) {
    boolean response =
        ElasticSearchUtil.upsertData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.usercourses.getTypeName(),
            id,
            courseMap);
    ProjectLogger.log(
        "UserCoursesService:sync: sync user courses id and  response  " + id + "==" + response,
        LoggerEnum.INFO.name());
  }

  public static void syncUserCourses(
      String courseId,
      String batchId,
      String enrolledOn,
      Integer progress,
      String lastAccessedOn,
      String userId) {
    Map<String, Object> userMap =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId);
    if (userMap != null) {
      List<Map<String, Object>> batches;
      if (userMap.get(JsonKey.BATCHES) != null) {
        batches = (List<Map<String, Object>>) userMap.get(JsonKey.BATCHES);
      } else {
        batches = new ArrayList<>();
      }
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.COURSE_ID, courseId);
      map.put(JsonKey.BATCH_ID, batchId);
      map.put(JsonKey.ENROLLED_ON, enrolledOn);
      map.put(JsonKey.PROGRESS, progress);
      map.put(JsonKey.LAST_ACCESSED_ON, lastAccessedOn);
      batches.add(map);
      userMap.put(JsonKey.BATCHES, batches);
    }
    boolean response =
        ElasticSearchUtil.upsertData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId,
            userMap);
    ProjectLogger.log(
        "UserCoursesService:syncUserCourses: sync user courses batch and  response  "
            + userId
            + "=="
            + response,
        LoggerEnum.INFO.name());
  }

  public static void syncRemoveUserCourses(String batchId, String userId) {
    Map<String, Object> userMap =
        ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId);
    if (userMap != null) {
      List<Map<String, Object>> batches;
      if (userMap.get(JsonKey.BATCHES) != null) {
        batches = (List<Map<String, Object>>) userMap.get(JsonKey.BATCHES);

        Iterator<Map<String, Object>> itr = batches.iterator();
        while (itr.hasNext()) {
          Map<String, Object> data = itr.next();
          String dataBatchId = (String) data.get(JsonKey.BATCH_ID);
          if (batchId.equalsIgnoreCase(dataBatchId)) {
            itr.remove();
          }
        }
        userMap.put(JsonKey.BATCHES, batches);
      }
    }
    boolean response =
        ElasticSearchUtil.upsertData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId,
            userMap);
    ProjectLogger.log(
        "UserCoursesService:syncRemoveUserCourses: sync user courses batch and  response  "
            + userId
            + "=="
            + response,
        LoggerEnum.INFO.name());
  }
}
