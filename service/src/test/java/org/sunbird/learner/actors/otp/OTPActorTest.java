package org.sunbird.learner.actors.otp;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class, CassandraOperationImpl.class})
@PowerMockIgnore("javax.management.*")
public class OTPActorTest {

  private TestKit probe;
  private ActorRef subject;

  private static final ActorSystem system = ActorSystem.create("system");
  private static final CassandraOperationImpl mockCassandraOperation =
      mock(CassandraOperationImpl.class);
  private static final Props props = Props.create(OTPActor.class);
  private static final String PHONE_TYPE = "phone";
  private static final String EMAIL_TYPE = "email";
  private static final String PHONE_KEY = "0000000000";
  private static final String EMAIL_KEY = "someEmail@someDomain.anything";
  private static final String REQUEST_OTP = "000000";
  private static final String INVALID_OTP = "111111";

  @BeforeClass
  public static void before() {
    PowerMockito.mockStatic(ServiceFactory.class);
    when(ServiceFactory.getInstance()).thenReturn(mockCassandraOperation);
  }

  @Before
  public void beforeEachTestCase() {
    probe = new TestKit(system);
    subject = system.actorOf(props);
  }

  @Test
  public void testVerifyOtpFailureWithInvalidPhoneOtp() {
    Response mockedCassandraResponse =
        getMockCassandraRecordByIdSuccessResponse(PHONE_KEY, PHONE_TYPE, INVALID_OTP);
    verifyOtpFailureTest(true, mockedCassandraResponse);
  }

  @Test
  public void testVerifyOtpFailureWithInvalidEmailOtp() {
    Response mockedCassandraResponse =
        getMockCassandraRecordByIdSuccessResponse(EMAIL_KEY, EMAIL_TYPE, INVALID_OTP);
    verifyOtpFailureTest(false, mockedCassandraResponse);
  }

  @Test
  public void testVerifyOtpFailureWithExpiredOtp() {
    Response mockedCassandraResponse = getMockCassandraRecordByIdFailureResponse();
    verifyOtpFailureTest(false, mockedCassandraResponse);
  }

  @Test
  public void testVerifyOtpSuccessWithPhoneOtp() {
    Response mockedCassandraResponse =
        getMockCassandraRecordByIdSuccessResponse(PHONE_KEY, PHONE_TYPE, REQUEST_OTP);
    verifyOtpSuccessTest(true, mockedCassandraResponse);
  }

  @Test
  public void testVerifyOtpSuccessWithEmailOtp() {
    Response mockedCassandraResponse =
        getMockCassandraRecordByIdSuccessResponse(EMAIL_KEY, EMAIL_TYPE, REQUEST_OTP);
    verifyOtpSuccessTest(false, mockedCassandraResponse);
  }

  private Request createRequestForVerifyOtp(String key, String type) {
    Request request = new Request();
    request.setOperation(ActorOperations.VERIFY_OTP.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.TYPE, type);
    innerMap.put(JsonKey.KEY, key);
    innerMap.put(JsonKey.OTP, REQUEST_OTP);
    request.setRequest(innerMap);
    return request;
  }

  private void verifyOtpSuccessTest(boolean isPhone, Response mockedCassandraResponse) {
    Request request;
    if (isPhone) {
      request = createRequestForVerifyOtp(PHONE_KEY, PHONE_TYPE);
    } else {
      request = createRequestForVerifyOtp(EMAIL_KEY, EMAIL_TYPE);
    }
    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(mockedCassandraResponse);
    subject.tell(request, probe.getRef());
    Response response = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(response.getResponseCode().equals(ResponseCode.OK));
  }

  private void verifyOtpFailureTest(boolean isPhone, Response mockedCassandraResponse) {
    Request request;
    if (isPhone) {
      request = createRequestForVerifyOtp(PHONE_KEY, PHONE_TYPE);
    } else {
      request = createRequestForVerifyOtp(EMAIL_KEY, EMAIL_TYPE);
    }
    when(mockCassandraOperation.getRecordById(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(mockedCassandraResponse);
    subject.tell(request, probe.getRef());
    ProjectCommonException exception =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(
        ((ProjectCommonException) exception)
            .getCode()
            .equals(ResponseCode.errorInvalidOTP.getErrorCode()));
  }

  private Response getMockCassandraRecordByIdSuccessResponse(String key, String type, String otp) {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> otpResponse = new HashMap<>();
    otpResponse.put(JsonKey.OTP, otp);
    otpResponse.put(JsonKey.TYPE, type);
    otpResponse.put(JsonKey.KEY, key);
    list.add(otpResponse);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private Response getMockCassandraRecordByIdFailureResponse() {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    response.put(JsonKey.RESPONSE, list);
    return response;
  }
}
