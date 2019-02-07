package org.sunbird.badge.dao;

import java.util.List;
import java.util.Map;
import org.sunbird.common.models.response.Response;

public interface ContentBadgeAssociationDao {

  /*
   * This method will insert new badge association list with content.
   *
   * @param Map<String, Object> contentBadgeDetails
   * @return Response
   */
  public Response insertBadgeAssociation(List<Map<String, Object>> contentInfoList);

  /*
   * This method will update content-badge association details.
   *
   * @param Map<String, Object> contentBadgeDetails
   * @return Response
   */
  public Response updateBadgeAssociation(Map<String, Object> updateMap);

  /*
   * This method will retrieve active content-badge association details.
   *
   * @param String contentId
   * @return List<Map<String, Object>> badgesInfo
   */
  public List<Map<String, Object>> getAllActiveAssociatedBadge(String contentId);

  /*
   * This method will retrieve all the content-badge association details.
   *
   * @param String contentId
   * @return List<Map<String, Object>> badgesInfo
   */
  public List<Map<String, Object>> getAllBadgeAssocaitedBadge(String contentId);

  public void createDataToES(Map<String, Object> badgeMap);

  public void updateDataToES(Map<String, Object> badgeMap);
}
