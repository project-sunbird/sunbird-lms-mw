package org.sunbird.location.dao;

import java.util.Map;
import org.sunbird.common.models.response.Response;
import org.sunbird.location.model.Location;

/** @author Amit Kumar */
public interface LocationDao {
  /**
   * @param location Location
   * @return response Response
   */
  public Response create(Location location);

  /**
   * @param location Location
   * @return response Response
   */
  public Response update(Location location);

  /**
   * @param locationId String
   * @return response Response
   */
  public Response delete(String locationId);

  /**
   * @param searchQueryMap Map<String,Object>
   * @return response Response
   */
  public Response search(Map<String, Object> searchQueryMap);

  /** @return response Response */
  public Response readAll();
}
