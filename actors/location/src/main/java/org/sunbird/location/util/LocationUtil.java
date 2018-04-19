package org.sunbird.location.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.sunbird.location.model.Location;

/** @author Amit Kumar */
public class LocationUtil {

  private LocationUtil() {}

  private static ObjectMapper mapper = new ObjectMapper();

  /**
   * This method will convert Map to Location Object
   *
   * @param map
   * @return location Location
   */
  public static Location convertMapToPojo(Map<String, Object> map) {
    return mapper.convertValue(map, Location.class);
  }

  /**
   * This method will convert Location Object to Map
   *
   * @param location Location
   * @return map Map
   */
  public static Map convertPojoToMap(Location location) {
    return mapper.convertValue(location, Map.class);
  }
}
