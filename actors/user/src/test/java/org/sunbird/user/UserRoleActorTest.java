package org.sunbird.user;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.sunbird.actor.core.BaseActorTest;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.user.actors.UserRoleActor;

public class UserRoleActorTest extends BaseActorTest {

  private ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(UserRoleActor.class);

  @Test
  public void testGetUserRoleSuccess() {
    assertTrue(testScenario(true, true, null));
  }

  @Test
  public void testAssignRolesSuccessWithValidOrgId() {
    assertTrue(testScenario(true, null));
  }

  @Test
  public void testAssignRolesSuccessWithoutOrgId() {
    assertTrue(testScenario(false, null));
  }

  @Test
  public void testAssignRolesFailure() throws Exception {
    resetAllMocks();
    assertTrue(testScenario(true, ResponseCode.CLIENT_ERROR));
  }

  @Test
  public void testAssignRolesFailureWithInvalidOrgId() {
    resetAllMocks();
    assertTrue(testScenario(false, ResponseCode.invalidParameterValue));
  }

  private boolean testScenario(boolean isOrgIdReq, ResponseCode errorResponse) {
    return testScenario(false, isOrgIdReq, errorResponse);
  }

  private boolean testScenario(
      boolean isGetUserRoles, boolean isOrgIdReq, ResponseCode errorResponse) {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    if (isGetUserRoles) {

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.GET_ROLES.getValue());
      subject.tell(reqObj, probe.getRef());
    } else {
      subject.tell(getRequestObj(isOrgIdReq), probe.getRef());
    }
    if (errorResponse == null) {
      Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
      return null != res && res.getResponseCode() == ResponseCode.OK;
    } else {
      ProjectCommonException res =
          probe.expectMsgClass(duration("1000 second"), ProjectCommonException.class);
      return res.getCode().equals(errorResponse.getErrorCode())
          || res.getResponseCode() == errorResponse.getResponseCode();
    }
  }

  @Override
  protected Map<String, Object> getOrganisationsMap() {

    Map<String, Object> orgMap = new HashMap<>();
    List<Map<String, Object>> list = new ArrayList<>();
    orgMap.put(JsonKey.ORGANISATION_ID, "ORGANISATION_ID");
    list.add(orgMap);
    orgMap.put(JsonKey.ORGANISATIONS, list);
    return orgMap;
  }

  @Override
  protected Map<String, Object> createResponseGet() {
    HashMap<String, Object> response = new HashMap<>();
    List<Map<String, Object>> content = new ArrayList<>();
    HashMap<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.CONTACT_DETAILS, "CONTACT_DETAILS");
    innerMap.put(JsonKey.ID, "ORGANISATION_ID");
    innerMap.put(JsonKey.HASHTAGID, "HASHTAGID");
    HashMap<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ORGANISATION_ID, "ORGANISATION_ID");
    List<Map<String, Object>> orgList = new ArrayList<>();
    orgList.add(orgMap);
    innerMap.put(JsonKey.ORGANISATIONS, orgList);
    content.add(innerMap);
    response.put(JsonKey.CONTENT, content);
    return response;
  }

  private Object getRequestObj(boolean isOrgIdReq) {
    Request reqObj = new Request();
    List roleLst = new ArrayList();
    roleLst.add("anyRole");
    reqObj.put(JsonKey.ROLES, roleLst);
    reqObj.put(JsonKey.EXTERNAL_ID, "EXTERNAL_ID");
    reqObj.put(JsonKey.USER_ID, "USER_ID");
    reqObj.put(JsonKey.HASHTAGID, "HASHTAGID");
    reqObj.put(JsonKey.PROVIDER, "PROVIDER");
    if (isOrgIdReq) {
      reqObj.put(JsonKey.ORGANISATION_ID, "ORGANISATION_ID");
    }
    reqObj.setOperation(ActorOperations.ASSIGN_ROLES.getValue());
    return reqObj;
  }

  protected static Response getCassandraResponse() {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, "ORGANISATION_ID");
    list.add(orgMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }
}
