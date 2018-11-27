package org.sunbird.learner.service.impl;

import java.util.List;
import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.service.OrgService;
import org.sunbird.learner.util.Util;

public class OrgServiceImpl implements OrgService {

  private Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
  private final CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static OrgService orgService = null;

  public static OrgService getInstance() {
    if (orgService == null) {
      orgService = new OrgServiceImpl();
    }
    return orgService;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean validateEmailUniqueness(Map<String, Object> orgMap, boolean isOrgCreate) {
    Response result =
        cassandraOperation.getRecordsByIndexedProperty(
            orgDbInfo.getKeySpace(),
            orgDbInfo.getTableName(),
            JsonKey.EMAIL,
            orgMap.get(JsonKey.EMAIL));
    List<Map<String, Object>> orgMapList = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
    if (!orgMapList.isEmpty()) {
      if (isOrgCreate) {
        ProjectCommonException.throwClientErrorException(ResponseCode.emailInUse);
      } else {
        Map<String, Object> orgDbMap = orgMapList.get(0);
        if (!(((String) orgDbMap.get(JsonKey.ID))
            .equalsIgnoreCase((String) orgMap.get(JsonKey.ORGANISATION_ID)))) {
          ProjectCommonException.throwClientErrorException(ResponseCode.emailInUse);
        }
      }
    }
    return true;
  }
}
