package org.sunbird.ratelimit.limiter;

/**
 * Rate limiter defines the variable to get limit for each unit(MINUTE, HOUR, DAY etc.) and
 * corresponding time duration ttl
 *
 * @author Rahul Kumar
 */
public enum OTP_RATE_LIMITER implements RATE_LIMITER {
  MINUTE("sunbird_otp_minute_rate_limit", 60),
  HOUR("sunbird_otp_hour_rate_limit", 3600),
  DAY("sunbird_otp_day_rate_limit", 86400);

  private String limitKey;
  private int ttl;

  private OTP_RATE_LIMITER(String limitKey, int ttl) {
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
