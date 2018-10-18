package org.sunbird.learner.actors.url.action.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.learner.actors.url.action.dao.UrlActionDao;
import org.sunbird.learner.actors.url.action.dao.impl.UrlActionDaoImpl;
import org.sunbird.models.url.action.UrlAction;

public class UrlActionService {

  private static UrlActionDao urlActionDao = UrlActionDaoImpl.getInstance();

  public static Map<String, Object> getUrlActionMap(String roleName) {
    List<UrlAction> urlActionList = urlActionDao.getUrlActions();
    Map<String, Object> response = new HashMap<>();
    if (urlActionList != null && !(urlActionList.isEmpty())) {
      for (UrlAction urlAction : urlActionList) {
        if (urlAction.getId().equals(roleName)) {
          response.put(JsonKey.ID, urlAction.getId());
          response.put(JsonKey.NAME, urlAction.getId());
          response.put(
              JsonKey.URL, urlAction.getUrl() != null ? urlAction.getUrl() : new ArrayList<>());
          return response;
        }
      }
    }
    return response;
  }
}
