package org.sunbird.user;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.user.actors.EducationManagementActor;
import org.sunbird.user.dao.EducationDao;
import org.sunbird.user.dao.impl.EducationDaoImpl;
import org.sunbird.user.util.UserActorOperations;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class, EducationDaoImpl.class})
@PowerMockIgnore({"javax.management.*"})
public class EducationManagementActorTest {

  private static final ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(EducationManagementActor.class);
  private static final CassandraOperation cassandraOperation = mock(CassandraOperationImpl.class);

  @Before
  public void beforetest() {
    PowerMockito.mockStatic(EducationDaoImpl.class);
    EducationDao educationDao = Mockito.mock(EducationDaoImpl.class);
    when(EducationDaoImpl.getInstance()).thenReturn(educationDao);
    when(educationDao.getPropertiesValueById(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getEducationResponse());
  }

  @BeforeClass
  public static void beforeClass() {

    PowerMockito.mockStatic(ServiceFactory.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    when(cassandraOperation.deleteRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getSuccessResponse());
  }

  @Test
  public void testInsertUserEducationSuccessWithRequiredParams() {

    boolean result =
        testScenario(
            UserActorOperations.INSERT_USER_EDUCATION,
            true,
            true,
            getUpsertResponse(),
            true,
            true,
            true);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserEducationSuccessForDeleteEducationDetails() {

    boolean result =
        testScenario(
            UserActorOperations.UPDATE_USER_EDUCATION,
            true,
            true,
            getUpsertResponse(),
            true,
            true,
            true);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserEducationSuccessForDeleteEducationDetailsWithoutAddress() {

    boolean result =
        testScenario(
            UserActorOperations.UPDATE_USER_EDUCATION,
            true,
            true,
            getUpsertResponse(),
            true,
            true,
            false);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserEducationSuccessWithoutId() {

    boolean result =
        testScenario(
            UserActorOperations.UPDATE_USER_EDUCATION,
            true,
            true,
            getUpsertResponse(),
            true,
            false,
            true);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserEducationSuccessForUpsertEducationDetails() {

    boolean result =
        testScenario(
            UserActorOperations.UPDATE_USER_EDUCATION,
            true,
            false,
            getUpsertResponse(),
            true,
            true,
            true);
    assertTrue(result);
  }

  @Test
  public void testInsertUserEducationFailureWithoutAddress() {

    boolean result =
        testScenario(
            UserActorOperations.INSERT_USER_EDUCATION,
            true,
            true,
            new Response(),
            false,
            false,
            false);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserEducationFailureForDeleteEducationDetailsWithoutParams() {

    boolean result =
        testScenario(
            UserActorOperations.UPDATE_USER_EDUCATION,
            false,
            true,
            new Response(),
            false,
            false,
            false);
    assertTrue(result);
  }

  private boolean testScenario(
      UserActorOperations operation,
      boolean isParamReq,
      boolean isDelete,
      Response response,
      boolean isSuccess,
      boolean isIdReq,
      boolean isAddressReq) {
    when(cassandraOperation.upsertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(response);

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(
        getRequestObject(operation, isParamReq, isDelete, isIdReq, isAddressReq), probe.getRef());
    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    if (isSuccess) {
      return res != null && "SUCCESS".equals(res.getResult().get(JsonKey.RESPONSE));
    } else {
      return res != null && res.getResult().get(JsonKey.ERROR_MSG) != null;
    }
  }

  private Response getUpsertResponse() {
    Response addrResponse = new Response();
    addrResponse.put(JsonKey.RESPONSE, "SUCCESS");
    return addrResponse;
  }

  private Request getRequestObject(
      UserActorOperations operation,
      boolean isParamReq,
      boolean isDelete,
      boolean isIdReq,
      boolean isAddressReq) {
    Request reqObj = new Request();
    reqObj.setOperation(operation.getValue());
    if (isParamReq) {
      reqObj.put(JsonKey.EDUCATION, getEducationList(isDelete, isIdReq, isAddressReq));
      reqObj.put(JsonKey.ID, "someId");
      reqObj.put(JsonKey.CREATED_BY, "createdBy");
    }
    return reqObj;
  }

  private Object getEducationList(boolean isDelete, boolean isIdReq, boolean isAddressReq) {

    List<Map<String, Object>> lst = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();

    Map<String, Object> addressmap = new HashMap<>();
    addressmap.put(JsonKey.ID, "someId");
    if (isAddressReq) {
      map.put(JsonKey.ADDRESS, addressmap);
    }
    if (isIdReq) {
      map.put(JsonKey.ID, "someUserId");
    }
    map.put(JsonKey.IS_DELETED, isDelete);
    lst.add(map);
    return lst;
  }

  private static Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, "SUCCESS");
    return response;
  }

  private static Response getEducationResponse() {
    Response response = new Response();
    List<Map<String, Object>> lst = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.ADDRESS_ID, "someAddressId");
    lst.add(map);
    response.put(JsonKey.RESPONSE, lst);
    return response;
  }
}
