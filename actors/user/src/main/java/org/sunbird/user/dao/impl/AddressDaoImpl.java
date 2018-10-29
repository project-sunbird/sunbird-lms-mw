package org.sunbird.user.dao.impl;

import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.user.dao.AddressDao;

public class AddressDaoImpl implements AddressDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);

  @Override
  public void createAddress(Map<String, Object> address) {
    address.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
    address.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    cassandraOperation.insertRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
  }

  @Override
  public void updateAddress(Map<String, Object> address) {
    address.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    address.remove(JsonKey.USER_ID);
    cassandraOperation.updateRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
  }

  @Override
  public void deleteAddress(String addressId) {
    cassandraOperation.deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addressId);
  }
}
