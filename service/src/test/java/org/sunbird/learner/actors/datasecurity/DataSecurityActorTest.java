package org.sunbird.learner.actors.datasecurity;

import static akka.testkit.JavaTestKit.duration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.data.security.manager.DataSecurityActor;

public class DataSecurityActorTest {

  private TestKit probe;
  private ActorRef subject;

  private static final ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(DataSecurityActor.class);
  private static final List<String> VALID_USER_IDS_LIST =
      Arrays.asList("validUserId1", "validUserId2");
  private static List<String> INVALID_USER_IDS_LIST;
  private static final String ENCRYPTION_OPERATION = ActorOperations.ENCRYPT_USER_DATA.getValue();
  private static final String DECRYPTION_OPERATION = ActorOperations.DECRYPT_USER_DATA.getValue();

  static {
    int maximumLimit =
        Integer.valueOf(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_USER_MAX_ENCRYPTION_LIMIT)) + 1;
    String userId = "someUserId";
    INVALID_USER_IDS_LIST = new ArrayList<>();
    for (int i = 0; i < maximumLimit; i++) {
      INVALID_USER_IDS_LIST.add(userId + i);
    }
  }

  @Before
  public void beforeEachTestCase() {
    probe = new TestKit(system);
    subject = system.actorOf(props);
  }

  @Test
  public void testEncryptDataFailureWithoutUserIds() {
    encryptionDecryptionFailureTest(
        false, null, ENCRYPTION_OPERATION, ResponseCode.mandatoryParamsMissing);
  }

  @Test
  public void testEncryptDataFailureWithUserIdsExceedsMaxAllowed() {
    encryptionDecryptionFailureTest(
        true, INVALID_USER_IDS_LIST, ENCRYPTION_OPERATION, ResponseCode.sizeLimitExceed);
  }

  @Test
  public void testEncryptDataSuccess() {
    encryptionDecryptionSuccessTest(true, VALID_USER_IDS_LIST, ENCRYPTION_OPERATION);
  }

  @Test
  public void testDecryptDataFailureWithoutUserIds() {
    encryptionDecryptionFailureTest(
        false, null, DECRYPTION_OPERATION, ResponseCode.mandatoryParamsMissing);
  }

  @Test
  public void testDecryptDataFailureWithUserIdsExceedsMaxAllowed() {
    encryptionDecryptionFailureTest(
        true, INVALID_USER_IDS_LIST, DECRYPTION_OPERATION, ResponseCode.sizeLimitExceed);
  }

  @Test
  public void testDecryptDataSuccess() {
    encryptionDecryptionSuccessTest(true, VALID_USER_IDS_LIST, DECRYPTION_OPERATION);
  }

  private Request createRequestForEncryption(
      boolean isUserIdsRequired, List<String> userIds, String operation) {
    Request request = new Request();
    request.setOperation(operation);
    Map<String, Object> innerMap = new HashMap<>();
    if (isUserIdsRequired) {
      innerMap.put(JsonKey.USER_IDs, userIds);
    }
    request.setRequest(innerMap);
    return request;
  }

  private void encryptionDecryptionSuccessTest(
      boolean isUserIdsRequired, List<String> userIds, String operation) {
    Request request = createRequestForEncryption(isUserIdsRequired, userIds, operation);
    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(response.getResponseCode().equals(ResponseCode.OK));
  }

  private void encryptionDecryptionFailureTest(
      boolean isUserIdsRequired,
      List<String> userIds,
      String operation,
      ResponseCode responseCode) {
    Request request = createRequestForEncryption(isUserIdsRequired, userIds, operation);
    subject.tell(request, probe.getRef());
    ProjectCommonException exception =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(
        ((ProjectCommonException) exception).getCode().equals(responseCode.getErrorCode()));
  }
}
