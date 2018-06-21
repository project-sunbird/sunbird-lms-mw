package org.sunbird.badge.util;

import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.badge.model.BadgeClassExtension;
import org.sunbird.badge.service.BadgeClassExtensionService;
import org.sunbird.badge.service.impl.BadgeClassExtensionServiceImpl;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.responsecode.ResponseCode;

/**
 * Class to provide validation for Assertion of badge
 *
 * @author arvind
 */
public class BadgeAssertionValidator {

  private static BadgeClassExtensionService badgeClassExtensionService =
      new BadgeClassExtensionServiceImpl();
  private static CassandraOperation cassandraOperation;
  private static final String USER_TABLE_NAME = "user";
  private static final String KEYSPACE_NAME = "sunbird";

  /**
   * Method to check if recipient type is user then root org of user and badge should be same
   *
   * @param recipientId represents the id of recipient to whom badge is going to assign
   * @param recipientType represents the type of recipient .Possible values are - user, content
   * @param badgeId represents the id of the badge
   */
  public static void checkBadgeAndRecipientFromSameRootOrg(
      String recipientId, String recipientType, String badgeId) {
    if (JsonKey.USER.equalsIgnoreCase(recipientType)) {
      String userRootOrg = getUserRootOrgId(recipientId);
      String badgeRootOrg = getBadgeRootOrgId(badgeId);
      if (!(StringUtils.equals(userRootOrg, badgeRootOrg))) {
        throw new ProjectCommonException(
            ResponseCode.commonAttributeMismatch.getErrorCode(),
            ResponseCode.commonAttributeMismatch.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode(),
            JsonKey.ROOT_ORG,
            BadgingJsonKey.BADGE_TYPE_USER,
            BadgingJsonKey.BADGE);
      }
    }
  }

  private static String getUserRootOrgId(String userId) {
    Response response = cassandraOperation.getRecordById(KEYSPACE_NAME, USER_TABLE_NAME, userId);
    List<Map<String, Object>> userResponse =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);

    if (CollectionUtils.isEmpty(userResponse)) {
      throw new ProjectCommonException(
          ResponseCode.userNotFound.getErrorCode(),
          ResponseCode.userNotFound.getErrorMessage(),
          ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
    }
    Map<String, Object> userData = userResponse.get(0);
    return (String) userData.get(JsonKey.ROOT_ORG_ID);
  }

  private static String getBadgeRootOrgId(String badgeId) {
    BadgeClassExtension badgeClassExtension = badgeClassExtensionService.get(badgeId);
    return badgeClassExtension.getRootOrgId();
  }
}
