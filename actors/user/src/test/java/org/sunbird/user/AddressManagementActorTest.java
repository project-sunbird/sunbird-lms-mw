package org.sunbird.user;

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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.user.actors.AddressManagementActor;
import org.sunbird.user.util.UserActorOperations;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  ElasticSearchUtil.class,
  EncryptionService.class,
  org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.class
})
@PowerMockIgnore({"javax.management.*"})
public class AddressManagementActorTest {

  private static ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(AddressManagementActor.class);
  private CassandraOperation cassandraOperation;

  @Before
  public void beforeEachTest() throws Exception {

    PowerMockito.mockStatic(ServiceFactory.class);
    PowerMockito.mockStatic(org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.class);
    EncryptionService encryptionService = Mockito.mock(EncryptionService.class);
    Mockito.when(
            org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
                .getEncryptionServiceInstance(null))
        .thenReturn(encryptionService);

    try {
      Mockito.when(encryptionService.encryptData(Mockito.anyString())).thenReturn("encrptUserId");
    } catch (Exception e) {
      Assert.fail("AddressManagementActorTest initialization failed");
    }
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
  }

  @Test
  public void testInsertUserAddressSuccess() {
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    testScenario(UserActorOperations.INSERT_USER_ADDRESS.getValue(), true);
  }

  @Test
  public void testInsertUserAddressFailureWithoutReqParams() {
    testScenario(UserActorOperations.INSERT_USER_ADDRESS.getValue(), false);
  }

  @Test
  public void testUpdateAddressSuccess() {
    when(cassandraOperation.deleteRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getSuccessResponse());
    testScenario(UserActorOperations.UPDATE_USER_ADDRESS.getValue(), true);
  }

  @Test
  public void testUpdateUserAddressFailureWithoutMandatoryFields() {
    testScenario(UserActorOperations.UPDATE_USER_ADDRESS.getValue(), true);
  }

  private void testScenario(String operation, boolean success) {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(getRequestObject(operation, success), probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    if (success) {
      Assert.assertTrue(res != null && res.getResult().get(JsonKey.RESPONSE) == "SUCCESS");
    } else {
      Assert.assertTrue(res != null && res.getResult().get(JsonKey.ERROR_MSG) != null);
    }
  }

  private Request getRequestObject(String operation, boolean withReqParams) {
    Request reqObj = new Request();
    reqObj.setOperation(operation);
    if (withReqParams) {
      reqObj.put(JsonKey.ADDRESS, getAddressList());
      reqObj.put(JsonKey.ID, "someId");
      reqObj.put(JsonKey.CREATED_BY, "createdBy");
    }
    return reqObj;
  }

  private List<Map<String, Object>> getAddressList() {

    List<Map<String, Object>> lst = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.ADDRESS, "anyAddress");
    map.put(JsonKey.ID, "someUserId");
    map.put(JsonKey.IS_DELETED, true);
    lst.add(map);
    return lst;
  }

  private Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, "SUCCESS");
    return response;
  }
}
