package org.sunbird.ratelimit.limiter;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.models.util.ProjectUtil;

/**
 * Rate limiter defines the variable to get limit for each unit(MINUTE, HOUR, DAY etc.) and
 * corresponding time duration ttl
 *
 * @author Rahul Kumar
 */
public enum OtpRateLimiter implements RateLimiter {
  MINUTE("sunbird_otp_minute_rate_limit", 60),
  HOUR("sunbird_otp_hour_rate_limit", 3600),
  DAY("sunbird_otp_day_rate_limit", 86400);

  private String limitKey;
  private int ttl;

  private OtpRateLimiter(String limitKey, int ttl) {
    this.limitKey = limitKey;
    this.ttl = ttl;
  }

  public String getLimitKey() {
    return limitKey;
  }

  public Integer getRateLimit() {
    String limitVal = ProjectUtil.getConfigValue(limitKey);
    if (!StringUtils.isBlank(limitVal)) {
      return Integer.valueOf(limitVal);
    }
    return null;
  }

  public int getTTL() {
    return ttl;
  }
}
