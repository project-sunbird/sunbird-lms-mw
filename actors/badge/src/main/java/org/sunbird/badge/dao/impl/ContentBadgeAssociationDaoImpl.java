package org.sunbird.badge.dao.impl;

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

  @Override
  public void createDataToES(Map<String, Object> badgeMap) {
    ElasticSearchUtil.createData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.badgeassociations.getTypeName(),
        (String) badgeMap.get(JsonKey.ID),
        badgeMap);
  }

  @Override
  public void updateDataToES(Map<String, Object> badgeMap) {
    ElasticSearchUtil.updateData(
        ProjectUtil.EsIndex.sunbird.getIndexName(),
        ProjectUtil.EsType.badgeassociations.getTypeName(),
        (String) badgeMap.get(JsonKey.ID),
        badgeMap);
  }
}
