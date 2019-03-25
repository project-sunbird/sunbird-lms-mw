package org.sunbird.learner.util;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.sunbird.common.models.util.JsonKey;

/** Created by rajatgupta on 25/03/19. */
public class UtilTest {

  @Test
  public void testValidateLimitMoreThan10000K() {

    Map<String, Object> requestObj = new HashMap<>();
    requestObj.put(JsonKey.LIMIT, 20000);
    Assert.assertTrue(250 == Util.createSearchDto(requestObj).getLimit());
  }

  @Test
  public void testValidateLimitAndOffsetMoreThan10000K() {

    Map<String, Object> requestObj = new HashMap<>();
    requestObj.put(JsonKey.LIMIT, 9989);
    requestObj.put(JsonKey.OFFSET, 20);
    Assert.assertTrue(250 == Util.createSearchDto(requestObj).getLimit());
  }
}
