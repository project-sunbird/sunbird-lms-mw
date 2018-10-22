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
  public void validateUserId(Request request) {
    String ctxtUserId = (String) request.getContext().get(JsonKey.USER_ID);
    String userId = userExtIdentityDao.getUserId(request);

    if ((!StringUtils.isBlank(userId)) && (!userId.equals(ctxtUserId))) {
      throw new ProjectCommonException(
          ResponseCode.unAuthorized.getErrorCode(),
          ResponseCode.unAuthorized.getErrorMessage(),
          ResponseCode.UNAUTHORIZED.getResponseCode());
    }
  }
}
