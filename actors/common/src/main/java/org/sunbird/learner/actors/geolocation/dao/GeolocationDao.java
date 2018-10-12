package org.sunbird.learner.actors.geolocation.dao;

import java.util.Map;
import org.sunbird.common.models.response.Response;
import org.sunbird.models.geolocation.Geolocation;

public interface GeolocationDao {

  /**
   * Get Geolocation information.
   *
   * @param id Identifier LocationId
   * @return Geolocation information
   */
  Geolocation read(String id);

  /**
   * Create an entry for Geolocation information
   *
   * @param geolocationDetails Geolocation information
   */
  Response insert(Map<String, Object> geolocationDetails);

  /**
   * Update Geolocation information
   *
   * @param updateAttributes Map containing Geolocation attributes which needs to be updated
   */
  Response update(Map<String, Object> updateAttributes);

  /**
   * Delete Geolocation information
   *
   * @param id geolocationId
   */
  Response delete(String id);
}
