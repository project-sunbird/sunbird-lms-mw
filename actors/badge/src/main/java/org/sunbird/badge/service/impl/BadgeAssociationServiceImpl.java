package org.sunbird.badge.service.impl;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.badge.dao.ContentBadgeAssociationDao;
import org.sunbird.badge.dao.impl.ContentBadgeAssociationDaoImpl;
import org.sunbird.badge.service.BadgeAssociationService;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;

public class BadgeAssociationServiceImpl implements BadgeAssociationService {

  private ContentBadgeAssociationDao associationDao = new ContentBadgeAssociationDaoImpl();

  @Override
  public String getBadgeAssociationId(String contentId, String badgeId, long timeStamp) {
    return OneWayHashing.encryptVal(
        contentId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + badgeId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + timeStamp);
  }

  @Override
  public Map<String, Object> getBadgeAssociationMapForContentUpdate(Map<String, Object> badgeMap) {
    Map<String, Object> associatedBadge = new HashMap<>();
    associatedBadge.put(BadgingJsonKey.BADGE_ID, badgeMap.get(BadgingJsonKey.BADGE_ID));
    associatedBadge.put(BadgingJsonKey.ISSUER_ID, badgeMap.get(BadgingJsonKey.ISSUER_ID));
    associatedBadge.put(BadgingJsonKey.ASSOCIATION_ID, badgeMap.get(BadgingJsonKey.ASSOCIATION_ID));
    associatedBadge.put(
        BadgingJsonKey.BADGE_CLASS_IMAGE, badgeMap.get(BadgingJsonKey.BADGE_CLASS_IMAGE));
    associatedBadge.put(BadgingJsonKey.CREATED_TS, badgeMap.get(BadgingJsonKey.CREATED_TS));
    associatedBadge.put(
        BadgingJsonKey.BADGE_CLASS_NANE, badgeMap.get(BadgingJsonKey.BADGE_CLASS_NANE));
    return associatedBadge;
  }

  public Map<String, Object> getCassandraBadgeAssociationCreateMap(
      Map<String, Object> badgeMap, String requestedBy, String contentId) {
    Map<String, Object> associatedBadgeMap = new HashMap<>();
    associatedBadgeMap.put(JsonKey.ID, badgeMap.get(BadgingJsonKey.ASSOCIATION_ID));
    associatedBadgeMap.put(JsonKey.CONTENT_ID, contentId);
    associatedBadgeMap.put(BadgingJsonKey.BADGE_ID, badgeMap.get(BadgingJsonKey.BADGE_ID));
    associatedBadgeMap.put(BadgingJsonKey.ISSUER_ID, badgeMap.get(BadgingJsonKey.ISSUER_ID));
    associatedBadgeMap.put(
        BadgingJsonKey.BADGE_CLASS_IMAGE, badgeMap.get(BadgingJsonKey.BADGE_CLASS_IMAGE));
    associatedBadgeMap.put(JsonKey.CREATED_ON, new Timestamp(new Date().getTime()));
    associatedBadgeMap.put(JsonKey.CREATED_BY, requestedBy);
    associatedBadgeMap.put(JsonKey.STATUS, true);
    associatedBadgeMap.put(
        BadgingJsonKey.BADGE_CLASS_NANE, badgeMap.get(BadgingJsonKey.BADGE_CLASS_NANE));
    return associatedBadgeMap;
  }

  @Override
  public Map<String, Object> getCassandraBadgeAssociationUpdateMap(
      String associationId, String requestedBy) {
    Map<String, Object> associatedBadgeMap = new HashMap<>();
    associatedBadgeMap.put(JsonKey.ID, associationId);
    associatedBadgeMap.put(JsonKey.LAST_UPDATED_BY, requestedBy);
    associatedBadgeMap.put(JsonKey.LAST_UPDATED_ON, new Timestamp(new Date().getTime()));
    associatedBadgeMap.put(JsonKey.STATUS, false);
    return associatedBadgeMap;
  }

  @Override
  public void syncToES(List<Map<String, Object>> badgeAssociationMapList, boolean toBeCreated) {
    for (Map<String, Object> badgeMap : badgeAssociationMapList) {
      if (toBeCreated) {
        associationDao.createDataToES(badgeMap);
      } else {
        associationDao.updateDataToES(badgeMap);
      }
    }
  }
}
