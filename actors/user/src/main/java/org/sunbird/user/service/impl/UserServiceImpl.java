package org.sunbird.user.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.user.dao.UserExternalIdentityDao;
import org.sunbird.user.dao.impl.UserExternalIdentityDaoImpl;
import org.sunbird.user.service.UserService;

public class UserServiceImpl implements UserService {

  private UserExternalIdentityDao userExtIdentityDao = new UserExternalIdentityDaoImpl();

  @Override
  public void validateWithUserId(Request reqObj) {
    if (null != reqObj.getContext().get(JsonKey.IS_AUTH_REQ)
        && Boolean.parseBoolean((String) reqObj.getContext().get(JsonKey.IS_AUTH_REQ))) {
      String ctxtUserId = (String) reqObj.getContext().get(JsonKey.USER_ID);
      String userId = userExtIdentityDao.getUserIdFromExtIdAndProvider(reqObj);
      if ((!StringUtils.isBlank(userId)) && (!userId.equals(ctxtUserId))) {
        throw new ProjectCommonException(
            ResponseCode.unAuthorized.getErrorCode(),
            ResponseCode.unAuthorized.getErrorMessage(),
            ResponseCode.UNAUTHORIZED.getResponseCode());
      }
    }
  }
}
