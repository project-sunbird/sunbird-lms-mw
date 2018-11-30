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
import org.sunbird.user.actors.UserProfileActor;
import org.sunbird.user.dao.UserOrgDao;
import org.sunbird.user.dao.impl.UserOrgDaoImpl;
import scala.concurrent.duration.Duration;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  RoleDaoImpl.class,
  BaseMWService.class,
  ActorSelection.class,
  ActorRef.class,
  RequestRouter.class,
  InterServiceCommunicationFactory.class,
  ElasticSearchUtil.class,
  Util.class,
  UserOrgDaoImpl.class,
  DecryptionService.class,
})
@PowerMockIgnore({"javax.management.*"})
public class UserProfileActorTest {

  private ActorSystem system = ActorSystem.create("system");

  private final Props props = Props.create(UserProfileActor.class);
  private CassandraOperationImpl cassandraOperation;
  private SearchDTO searchDTO;
  private static InterServiceCommunication interServiceCommunication =
      Mockito.mock(InterServiceCommunication.class);

  private Response response = Mockito.mock(Response.class);
  private static DecryptionService decryptionService;
  private static String userId = "USER-ID";

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

    RoleDaoImpl roleDao = Mockito.mock(RoleDaoImpl.class);
    when(RoleDaoImpl.getInstance()).thenReturn(roleDao);

    UserOrgDao userOrgDao = Mockito.mock(UserOrgDaoImpl.class);
    when(UserOrgDaoImpl.getInstance()).thenReturn(userOrgDao);
    when(userOrgDao.updateUserOrg(Mockito.anyObject())).thenReturn(getSuccessResponse());
    when(InterServiceCommunicationFactory.getInstance()).thenReturn(interServiceCommunication);

    ActorSelection actorSelection = Mockito.mock(ActorSelection.class);
    CompletionStage completionStage = Mockito.mock(CompletionStage.class);
    when(BaseMWService.getRemoteRouter(Mockito.anyString())).thenReturn(actorSelection);
    when(actorSelection.resolveOneCS(Duration.create(Mockito.anyLong(), "seconds")))
        .thenReturn(completionStage);

    ActorRef actorRef = Mockito.mock(ActorRef.class);
    when(RequestRouter.getActor(Mockito.anyString())).thenReturn(actorRef);

    searchDTO = Mockito.mock(SearchDTO.class);
    when(Util.createSearchDto(Mockito.anyMap())).thenReturn(searchDTO);

    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
  }

  @Test
  public void testGetMediaTypesSuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.GET_MEDIA_TYPES.getValue());
    when(cassandraOperation.getAllRecords(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getSuccessResponse());

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testSetProfileVisibilitySuccess() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId))
        .thenReturn(createGetResponse(true));
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.PROFILE_VISIBILITY.getValue());
    reqObj.put(JsonKey.USER_ID, userId);
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(null != res && res.getResponseCode() == ResponseCode.OK);
  }

  @Test
  public void testSetProfileVisibilityFailure() {

    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    when(ElasticSearchUtil.getDataByIdentifier(
            ProjectUtil.EsIndex.sunbird.getIndexName(),
            ProjectUtil.EsType.user.getTypeName(),
            userId))
        .thenReturn(createGetResponse(false));
    Request reqObj = new Request();
    reqObj.setOperation(ActorOperations.PROFILE_VISIBILITY.getValue());
    reqObj.put(JsonKey.USER_ID, userId);
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
    Assert.assertTrue(res.getCode() == ResponseCode.userNotFound.getErrorCode());
  }

  private Map<String, Object> createGetResponse(boolean isSuccess) {
    HashMap<String, Object> response = new HashMap<>();
    if (isSuccess) response.put(JsonKey.CONTENT, "Any-content");
    return response;
  }

  private Response getSuccessResponse() {
    Response response = new Response();
    List<Map<String, Object>> resMapList = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    resMapList.add(map);
    response.put(JsonKey.RESPONSE, resMapList);
    return response;
  }
}
