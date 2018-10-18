package org.sunbird.user.dao;

import org.sunbird.common.request.Request;

public interface UserExternalIdentityDao {

  public String getUserIdFromExtIdAndProvider(Request reqObj);
}
