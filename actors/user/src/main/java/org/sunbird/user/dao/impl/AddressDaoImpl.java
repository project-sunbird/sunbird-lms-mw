package org.sunbird.user.dao.impl;

import java.util.Map;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.user.dao.AddressDao;

public class AddressDaoImpl implements AddressDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private Util.DbInfo addrDbInfo = Util.dbInfoMap.get(JsonKey.ADDRESS_DB);

  private static AddressDao addressDao = null;

  private AddressDaoImpl() {}

  public static AddressDao getInstance() {
    if (null == addressDao) {
      addressDao = new AddressDaoImpl();
    }
    return addressDao;
  }

  @Override
  public void createAddress(Map<String, Object> address) {
    cassandraOperation.insertRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
  }

  @Override
  public void updateAddress(Map<String, Object> address) {
    cassandraOperation.updateRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
  }

  @Override
  public void deleteAddress(String addressId) {
    cassandraOperation.deleteRecord(addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), addressId);
  }

  @Override
  public Response upsertAddress(Map<String, Object> address) {
    return cassandraOperation.upsertRecord(
        addrDbInfo.getKeySpace(), addrDbInfo.getTableName(), address);
  }
}
