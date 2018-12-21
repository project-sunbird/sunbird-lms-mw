package org.sunbird.ratelimit.service;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.ratelimit.dao.RateLimitDao;
import org.sunbird.ratelimit.limiter.OtpRateLimiter;
import org.sunbird.ratelimit.limiter.RateLimit;
import org.sunbird.ratelimit.limiter.RateLimiter;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*"})
public class RateLimitServiceTest {

  private static final String KEY = "9999888898";
  private static final int HOUR_LIMIT = 10;

  @InjectMocks private RateLimitService rateLimitService = new RateLimitServiceImpl();

  @Mock private RateLimitDao rateLimitdDao;

  @Mock private RateLimiter hourRateLimiter = OtpRateLimiter.HOUR;

  @Mock private RateLimiter dayRateLimiter = OtpRateLimiter.DAY;

  @Before
  public void beforeEachTest() {
    MockitoAnnotations.initMocks(this);
    when(hourRateLimiter.getRateLimit()).thenReturn(10);
    when(dayRateLimiter.getRateLimit()).thenReturn(50);
    when(hourRateLimiter.name()).thenReturn(OtpRateLimiter.HOUR.name());
    when(dayRateLimiter.name()).thenReturn(OtpRateLimiter.DAY.name());
    when(dayRateLimiter.getRateLimit()).thenReturn(50);
    doNothing().when(rateLimitdDao).insertRateLimits(anyList());
  }

  @Test
  public void testThrottleByKeyOngoing() {
    when(rateLimitdDao.getRateLimits(anyString())).thenReturn(getRateLimitRecords());
    doAnswer(
            (Answer)
                invocation -> {
                  List<RateLimit> rateLimits = invocation.getArgumentAt(0, List.class);
                  assertTrue(CollectionUtils.isNotEmpty(rateLimits));
                  assertSame(1, rateLimits.size());
                  assertSame(6, rateLimits.get(0).getCount());
                  return null;
                })
        .when(rateLimitdDao)
        .insertRateLimits(anyList());
    rateLimitService.throttleByKey(KEY, new RateLimiter[] {hourRateLimiter});
  }

  @Test
  public void testThrottleByKeyNew() {
    when(rateLimitdDao.getRateLimits(anyString())).thenReturn(null);
    doAnswer(
            (Answer)
                invocation -> {
                  List<RateLimit> rateLimits = invocation.getArgumentAt(0, List.class);
                  assertTrue(CollectionUtils.isNotEmpty(rateLimits));
                  assertSame(1, rateLimits.size());
                  assertSame(1, rateLimits.get(0).getCount());
                  return null;
                })
        .when(rateLimitdDao)
        .insertRateLimits(anyList());
    rateLimitService.throttleByKey(KEY, new RateLimiter[] {hourRateLimiter});
  }

  @Test
  public void testThrottleByKeyMultipleLimit() {
    when(rateLimitdDao.getRateLimits(anyString())).thenReturn(getRateLimitRecords());
    doAnswer(
            (Answer)
                invocation -> {
                  List<RateLimit> rateLimits = invocation.getArgumentAt(0, List.class);
                  assertTrue(CollectionUtils.isNotEmpty(rateLimits));
                  assertSame(2, rateLimits.size());
                  rateLimits.forEach(
                      rateLimit -> {
                        if (hourRateLimiter.name().equals(rateLimit.getUnit())) {
                          assertSame(6, rateLimit.getCount());
                        }
                        if (dayRateLimiter.name().equals(rateLimit.getUnit())) {
                          assertSame(1, rateLimit.getCount());
                        }
                      });
                  return null;
                })
        .when(rateLimitdDao)
        .insertRateLimits(anyList());
    rateLimitService.throttleByKey(KEY, new RateLimiter[] {hourRateLimiter, dayRateLimiter});
  }

  @Test(expected = ProjectCommonException.class)
  public void testThrottleByKeyFailure() {
    when(rateLimitdDao.getRateLimits(anyString())).thenReturn(getRateLimitMaxCountRecords());
    rateLimitService.throttleByKey(KEY, new RateLimiter[] {hourRateLimiter});
  }

  private List<RateLimit> getRateLimits() {
    List<RateLimit> rateLimits = new ArrayList<>();
    rateLimits.add(
        new RateLimit(KEY, OtpRateLimiter.HOUR.name(), 20, OtpRateLimiter.HOUR.getTTL()));
    return rateLimits;
  }

  private List<RateLimit> getInvalidRateLimits() {
    List<RateLimit> rateLimits = new ArrayList<>();
    rateLimits.add(new RateLimit(KEY, null, 0, OtpRateLimiter.HOUR.getTTL()));
    return rateLimits;
  }

  private List<Map<String, Object>> getRateLimitRecords() {
    List<Map<String, Object>> results = new ArrayList<>();
    Map<String, Object> record = new HashMap<>();
    record.put(JsonKey.KEY, KEY);
    record.put(JsonKey.RATE_LIMIT_UNIT, OtpRateLimiter.HOUR.name());
    record.put(JsonKey.RATE, HOUR_LIMIT);
    record.put(JsonKey.TTL, 3500);
    record.put(JsonKey.COUNT, 5);
    results.add(record);
    return results;
  }

  private List<Map<String, Object>> getRateLimitMaxCountRecords() {
    List<Map<String, Object>> results = new ArrayList<>();
    Map<String, Object> record = new HashMap<>();
    record.put(JsonKey.KEY, KEY);
    record.put(JsonKey.RATE_LIMIT_UNIT, OtpRateLimiter.HOUR.name());
    record.put(JsonKey.RATE, HOUR_LIMIT);
    record.put(JsonKey.TTL, 3500);
    record.put(JsonKey.COUNT, HOUR_LIMIT);
    results.add(record);
    return results;
  }
}
