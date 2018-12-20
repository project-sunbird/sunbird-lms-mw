/** */
package org.sunbird.ratelimit.service;

/** @author Rahul Kumar */
public interface RateLimitService {

  /**
   * validates the rate limit in below steps
   *
   * <ol>
   *   <li>checks if global rate limiting is on
   *   <li>fetches the existing records and verifies if limit has been crossed for each rate limiter
   *   <li>upsert entries for each rate limiter with counts
   * </ol>
   *
   * @param key the target value on which rate limits should be applied
   * @param rateLimiters an array of rate limit to be validated against ex. HOUR, DAY etc.
   */
  void validateRate(String key, RATE_LIMITER[] rateLimiters);
}
