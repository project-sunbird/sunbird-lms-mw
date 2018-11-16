package org.sunbird.user;

import static akka.testkit.JavaTestKit.duration;
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
import org.junit.Assert;
import org.junit.Before;
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
import org.sunbird.actorutil.org.impl.OrganisationClientImpl;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.role.dao.impl.RoleDaoImpl;
import org.sunbird.models.organisation.Organisation;
import org.sunbird.user.actors.UserRoleActor;
import scala.concurrent.duration.Duration;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  RoleDaoImpl.class,
  BaseMWService.class,
  ActorSelection.class,
  ActorRef.class,
  RequestRouter.class,
  InterServiceCommunication.class,
  Organisation.class,
  OrganisationClientImpl.class,
  InterServiceCommunicationFactory.class,
  Response.class
})
@PowerMockIgnore({"javax.management.*"})
public class UserRoleActorTest {

  private static ActorSystem system;
  private static final Props props = Props.create(UserRoleActor.class);
  private static CassandraOperationImpl cassandraOperation;
  private static RoleDaoImpl roleDao;
  private static ActorSelection actorSelection;
  private static CompletionStage completionStage;
  private static ActorRef actorRef;
  private static InterServiceCommunication interServiceCommunication;
  private static Organisation organisation;
  private static Response response;
  private static Map<String, Object> map;

  @Before
  public void beforeEachTest() {

    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    PowerMockito.mockStatic(RoleDaoImpl.class);
    roleDao = Mockito.mock(RoleDaoImpl.class);
    when(RoleDaoImpl.getInstance()).thenReturn(roleDao);
    system = ActorSystem.create("system");
    PowerMockito.mockStatic(ServiceFactory.class);
    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    actorSelection = Mockito.mock(ActorSelection.class);
    PowerMockito.mockStatic(BaseMWService.class);
    completionStage = Mockito.mock(CompletionStage.class);
    PowerMockito.mockStatic(RequestRouter.class);
    actorRef = Mockito.mock(ActorRef.class);

    PowerMockito.mockStatic(InterServiceCommunicationFactory.class);
    interServiceCommunication = Mockito.mock(InterServiceCommunication.class);
    when(InterServiceCommunicationFactory.getInstance()).thenReturn(interServiceCommunication);
    organisation = Mockito.mock(Organisation.class);
    response = Mockito.mock(Response.class);
    map = Mockito.mock(Map.class);
  }

  @Test
  public void testGetUserRoleSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Response response = getCassandraResponse();
    when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_ROLES.getValue());
    subject.tell(reqObj, probe.getRef());

    Response res = probe.expectMsgClass(duration("10 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testAssignRolesSuccessWithValidRole() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(BaseMWService.getRemoteRouter(Mockito.anyString())).thenReturn(actorSelection);

    when(actorSelection.resolveOneCS(Duration.create(Mockito.anyLong(), "seconds")))
        .thenReturn(completionStage);
    when(RequestRouter.getActor(Mockito.anyString())).thenReturn(actorRef);

    when(interServiceCommunication.getResponse(Mockito.anyObject(), Mockito.anyObject()))
        .thenReturn(response);
    when(response.get(Mockito.anyString())).thenReturn(map);

    //        when(interServiceCommunication.getResponse(Mockito.anyObject(),
    // Mockito.anyObject())).thenReturn(organisation);

    Request reqObj = new Request();
    List roleLst = new ArrayList();
    roleLst.add("anyRole");
    reqObj.put(JsonKey.ROLES, roleLst);
    reqObj.put(JsonKey.EXTERNAL_ID, "EXTERNAL_ID");
    reqObj.put(JsonKey.USER_ID, "USER_ID");
    reqObj.put(JsonKey.HASHTAGID, "HASHTAGID");
    reqObj.put(JsonKey.PROVIDER, "PROVIDER");
    reqObj.put(JsonKey.ORGANISATION_ID, "ORGANISATION_ID");
    reqObj.setOperation(ActorOperations.ASSIGN_ROLES.getValue());
    subject.tell(reqObj, probe.getRef());

    Response res = probe.expectMsgClass(duration("10000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testAssignRolesFailure() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    Response response = getCassandraResponse();
    when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(response);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_ROLES.getValue());
    subject.tell(reqObj, probe.getRef());

    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(null != res);
  }

  private Response getCassandraResponse() {

    Response response = new Response();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> orgMap = new HashMap<>();
    orgMap.put(JsonKey.ID, "org123");
    list.add(orgMap);
    response.put(JsonKey.RESPONSE, list);
    return response;
  }
}
