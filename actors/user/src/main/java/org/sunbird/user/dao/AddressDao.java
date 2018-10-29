package org.sunbird.user.dao;

import java.util.Map;

public interface AddressDao {

  void createAddress(Map<String, Object> address);

  void updateAddress(Map<String, Object> address);

  void deleteAddress(String addressId);
}
