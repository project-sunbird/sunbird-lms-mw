package org.sunbird.ratelimit.service;

/**
 * Rate limiter defines the variable to get limit for each unit(MINUTE, HOUR, DAY etc.) and
 * corresponding time duration ttl
 *
 * @author Rahul Kumar
 */
public enum RATE_LIMITER {
  MINUTE("sunbird_hour_rate_limit", 60),
  HOUR("sunbird_hour_rate_limit", 3600),
  DAY("sunbird_day_rate_limit", 86400);

  private String limitKey;
  private int ttl;

  private RATE_LIMITER(String limitKey, int ttl) {
    this.limitKey = limitKey;
    this.ttl = ttl;
  }

  public String getLimitKey() {
    return limitKey;
  }

  public int getTtl() {
    return ttl;
  }
}
