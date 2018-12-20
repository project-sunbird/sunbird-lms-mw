package org.sunbird.ratelimit.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.ratelimit.dao.RateLimitDao;
import org.sunbird.ratelimit.dao.RateLimitDaoImpl;

public class RateLimitServiceImpl implements RateLimitService {

  private RateLimitDao rateLimitDao = RateLimitDaoImpl.getInstance();

  public boolean isRateLimitOn() {
    return Boolean.TRUE
        .toString()
        .equalsIgnoreCase(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_RATE_LIMIT_ENABLED));
  }

  @Override
  public void validateRate(String key, RATE_LIMITER[] rateLimiters) {
    if (!isRateLimitOn()) {
      ProjectLogger.log(
          "RateLimitServiceImpl:validateRate rate limiter is disabled", LoggerEnum.INFO);
      return;
    }
    Map<RATE_LIMITER, Map<String, Object>> entryByRate = new HashMap<>();

    List<Map<String, Object>> ratesByKey = getRatesByKey(key);
    if (!CollectionUtils.isEmpty(ratesByKey)) {
      ratesByKey
          .stream()
          .forEach(
              rate -> {
                if (!MapUtils.isEmpty(rate)) {
                  ProjectLogger.log(
                      "RateLimitServiceImpl:validateRate key =" + key + " rate =" + rate,
                      LoggerEnum.INFO);
                  String unit = (String) rate.get(JsonKey.RATE_LIMIT_UNIT);
                  RATE_LIMITER rateLimiter = RATE_LIMITER.valueOf(unit);
                  String limitVal = ProjectUtil.getConfigValue(rateLimiter.getLimitKey());
                  if (!StringUtils.isBlank(limitVal)) {
                    int limit = Integer.valueOf(limitVal);
                    Integer count = (int) rate.get(JsonKey.COUNT);
                    if (count >= limit) {
                      ProjectLogger.log(
                          "RateLimitServiceImpl:validateRate  rate limit threshold crossed for key ="
                              + key,
                          LoggerEnum.ERROR);
                      throw new ProjectCommonException(
                          ResponseCode.rateLimitExceeded.getErrorCode(),
                          ResponseCode.rateLimitExceeded.getErrorMessage(),
                          ResponseCode.FORBIDDEN.getResponseCode(),
                          rateLimiter.name().toLowerCase());
                    }
                    rate.put(JsonKey.COUNT, count + 1);
                    entryByRate.put(rateLimiter, rate);
                  }
                }
              });
    }

    Arrays.stream(rateLimiters)
        .forEach(
            rateLimiter -> {
              if (!entryByRate.containsKey(rateLimiter)) {
                Map<String, Object> entry = new HashMap<>();
                entry.put(JsonKey.KEY, key);
                entry.put(JsonKey.RATE_LIMIT_UNIT, rateLimiter.name());
                entry.put(JsonKey.COUNT, 1);
                entry.put(JsonKey.TTL, rateLimiter.getTtl());
                ProjectLogger.log(
                    "RateLimitServiceImpl:validateRate insert for key =" + key + " rate =" + entry,
                    LoggerEnum.INFO);
                entryByRate.put(rateLimiter, entry);
              }
            });

    rateLimitDao.insertRateLimits(new ArrayList<>(entryByRate.values()));
  }

  private List<Map<String, Object>> getRatesByKey(String key) {
    return rateLimitDao.getRateLimits(key);
  }
}
