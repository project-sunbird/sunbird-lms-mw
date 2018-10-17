package org.sunbird.learner.actors.url.action.dao;

import java.util.List;
import org.sunbird.models.url.action.UrlAction;

public interface UrlActionDao {

  /**
   * Get All UrlActions
   *
   * @return List of all UrlActions from Url_action Table
   */
  List<UrlAction> getUrlActions();
}
