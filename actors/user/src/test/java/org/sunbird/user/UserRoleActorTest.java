package org.sunbird.user;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.actor.service.BaseMWService;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.role.dao.impl.RoleDaoImpl;
import org.sunbird.learner.util.Util;
import org.sunbird.user.actors.UserRoleActor;
import org.sunbird.user.dao.UserOrgDao;
import org.sunbird.user.dao.impl.UserOrgDaoImpl;
import scala.concurrent.duration.Duration;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  RoleDaoImpl.class,
  BaseMWService.class,
  RequestRouter.class,
  InterServiceCommunicationFactory.class,
  ElasticSearchUtil.class,
  Util.class,
  UserOrgDaoImpl.class,
  DecryptionService.class,
})
@PowerMockIgnore({"javax.management.*"})
public class UserRoleActorTest {

  private ActorSystem system = ActorSystem.create("system");
  private static final Props props = Props.create(UserRoleActor.class);
  private static SearchDTO searchDTO;
  private static InterServiceCommunication interServiceCommunication =
      Mockito.mock(InterServiceCommunication.class);
  private static Response response = Mockito.mock(Response.class);

  @BeforeClass
  public static void beforeClass() {
    PowerMockito.mockStatic(ServiceFactory.class);
    CassandraOperationImpl cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getCassandraResponse());
  }

  @Before
  public void beforeEachTest() {

    PowerMockito.mockStatic(ServiceFactory.class);
    PowerMockito.mockStatic(BaseMWService.class);
    PowerMockito.mockStatic(RequestRouter.class);
    PowerMockito.mockStatic(InterServiceCommunicationFactory.class);
    PowerMockito.mockStatic(RoleDaoImpl.class);
    PowerMockito.mockStatic(ElasticSearchUtil.class);
    PowerMockito.mockStatic(Util.class);
    PowerMockito.mockStatic(UserOrgDaoImpl.class);
    when(InterServiceCommunicationFactory.getInstance()).thenReturn(interServiceCommunication);
    RoleDaoImpl roleDao = Mockito.mock(RoleDaoImpl.class);
    when(RoleDaoImpl.getInstance()).thenReturn(roleDao);
    UserOrgDao userOrgDao = Mockito.mock(UserOrgDaoImpl.class);
    when(UserOrgDaoImpl.getInstance()).thenReturn(userOrgDao);
    when(userOrgDao.updateUserOrg(Mockito.anyObject())).thenReturn(getSuccessResponse());
    CompletionStage completionStage = Mockito.mock(CompletionStage.class);
    ActorSelection actorSelection = Mockito.mock(ActorSelection.class);
    when(BaseMWService.getRemoteRouter(Mockito.anyString())).thenReturn(actorSelection);
    when(actorSelection.resolveOneCS(Duration.create(Mockito.anyLong(), "seconds")))
        .thenReturn(completionStage);
    ActorRef actorRef = Mockito.mock(ActorRef.class);
    when(RequestRouter.getActor(Mockito.anyString())).thenReturn(actorRef);
    searchDTO = Mockito.mock(SearchDTO.class);
    when(Util.createSearchDto(Mockito.anyMap())).thenReturn(searchDTO);
  }

  @Test
  public void testGetUserRoleSuccess() {
    assertTrue(testScenario(true, response, true, true, true, null));
  }

  @Test
  public void testAssignRolesSuccessWithValidOrgId() {
    assertTrue(testScenario(false, response, true, true, true, null));
  }

  @Test
  public void testAssignRolesSuccessWithoutOrgId() {
    assertTrue(testScenario(false, response, true, false, true, null));
  }

  @Test
  public void testAssignRolesFailure() {
    assertTrue(testScenario(false, null, false, true, false, null));
  }

  @Test
  public void testAssignRolesFailureWithInvalidOrgId() {
    assertTrue(testScenario(false, null, true, true, false, ResponseCode.invalidParameterValue));
  }

  private boolean testScenario(
      boolean isGetUserRoles,
      Response response,
      boolean isResponseRequired,
      boolean isObjRequred,
      boolean isSuccess,
      ResponseCode errorResponse) {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    if (isGetUserRoles) {
      //      CassandraOperationImpl cassandraOperation = mock(CassandraOperationImpl.class);
      //      when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
      //      when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
      //          .thenReturn(getCassandraResponse());

      Request reqObj = new Request();
      reqObj.setOperation(ActorOperations.GET_ROLES.getValue());
      subject.tell(reqObj, probe.getRef());
    } else {
      DecryptionService decryptionService = Mockito.mock(DecryptionService.class);
      when(decryptionService.decryptData(Mockito.anyMap())).thenReturn(getOrganisationsMap());

      when(interServiceCommunication.getResponse(Mockito.anyObject(), Mockito.anyObject()))
          .thenReturn(response);
      if (response != null) when(response.get(Mockito.anyString())).thenReturn(new HashMap<>());

      mockGetOrgResponse(isResponseRequired);
      subject.tell(getRequestObj(isObjRequred), probe.getRef());
    }
    if (isSuccess) {
      Response res = probe.expectMsgClass(duration("10 second"), Response.class);
      return null != res && res.getResponseCode() == ResponseCode.OK;
    } else {
      ProjectCommonException res =
          probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);

      if (errorResponse != null) {
        return res.getCode().equals(errorResponse.getErrorCode());
      } else return res.getResponseCode() == 400;
    }
  }

  private Map<String, Object> getOrganisationsMap() {

    Map<String, Object> orgMap = new HashMap<>();
    List<Map<String, Object>> list = new ArrayList<>();
    orgMap.put(JsonKey.ORGANISATION_ID, "ORGANISATION_ID");
    list.add(orgMap);
    orgMap.put(JsonKey.ORGANISATIONS, list);
    return orgMap;
  }

  private Map<String, Object> createResponseGet(boolean isSuccess) {
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
    if (isSuccess) {
      content.add(innerMap);
    }
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
    if (isOrgIdReq) reqObj.put(JsonKey.ORGANISATION_ID, "ORGANISATION_ID");
    reqObj.setOperation(ActorOperations.ASSIGN_ROLES.getValue());
    return reqObj;
  }

  private void mockGetOrgResponse(boolean isSuccess) {

    searchDTO = Mockito.mock(SearchDTO.class);
    when(Util.createSearchDto(Mockito.anyMap())).thenReturn(searchDTO);
    when(ElasticSearchUtil.complexSearch(
            Mockito.any(SearchDTO.class),
            Mockito.eq(ProjectUtil.EsIndex.sunbird.getIndexName()),
            Mockito.anyVararg()))
        .thenReturn(createResponseGet(isSuccess));
  }

  private static Response getCassandraResponse() {
    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, "ORGANISATION_ID");
    list.add(orgMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }

  private Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }
}
