package org.sunbird.ratelimit.limiter;

public interface RATE_LIMITER {

  String getLimitKey();

  int getTtl();

  String name();
}
