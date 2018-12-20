package org.sunbird.ratelimit.dao;

import java.util.List;
import java.util.Map;

public interface RateLimitDao {

  /**
   * batch inserts the rate limits
   *
   * @param rateLimits list of records of rate limit
   */
  void insertRateLimits(List<Map<String, Object>> rateLimits);

  /**
   * fetches all the rate limits applicable for the key
   *
   * @param key
   * @return List of rate limits records for the key
   */
  List<Map<String, Object>> getRateLimits(String key);
}
