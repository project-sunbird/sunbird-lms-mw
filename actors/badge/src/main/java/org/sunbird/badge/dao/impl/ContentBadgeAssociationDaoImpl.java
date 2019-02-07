package org.sunbird.badge.dao.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.badge.dao.ContentBadgeAssociationDao;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.helper.ServiceFactory;

public class ContentBadgeAssociationDaoImpl implements ContentBadgeAssociationDao {

  private static final String KEYSPACE = "sunbird" + "";
  private static final String TABLE_NAME = "content_badge_association";
  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public Response insertBadgeAssociation(List<Map<String, Object>> contentInfo) {
    return cassandraOperation.batchInsert(KEYSPACE, TABLE_NAME, contentInfo);
  }

  @Override
  public Response updateBadgeAssociation(Map<String, Object> updateMap) {
    return cassandraOperation.updateRecord(KEYSPACE, TABLE_NAME, updateMap);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Map<String, Object>> esGetAllActiveAssociatedBadges(String contentId) {
    Map<String, Object> searchMap = new HashMap<>();
    searchMap.put(JsonKey.CONTENT_ID, contentId);
    searchMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
    Map<String, Object> result =
        ElasticSearchUtil.searchData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.badge.getTypeName(),
            searchMap);
    return (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Map<String, Object>> esGetAllAssociatedBadges(String contentId) {
    Map<String, Object> searchMap = new HashMap<>();
    searchMap.put(JsonKey.CONTENT_ID, contentId);
    Map<String, Object> result =
        ElasticSearchUtil.searchData(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.badge.getTypeName(),
            searchMap);
    return (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
  }

  @Override
  public void createDataToES(Map<String, Object> badgeMap) {
    ElasticSearchUtil.createData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.badge.getTypeName(),
        (String) badgeMap.get(JsonKey.ID),
        badgeMap);
  }

  @Override
  public void updateDataToES(Map<String, Object> badgeMap) {
    ElasticSearchUtil.updateData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.badge.getTypeName(),
        (String) badgeMap.get(JsonKey.ID),
        badgeMap);
  }
}
