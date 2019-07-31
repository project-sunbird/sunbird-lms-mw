package org.sunbird.user;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.Before;
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
import org.sunbird.models.user.User;
import org.sunbird.user.actors.UserMergeActor;
import org.sunbird.user.dao.impl.UserDaoImpl;
import org.sunbird.user.service.impl.UserServiceImpl;

import java.util.HashMap;
import java.util.Map;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        UserServiceImpl.class,
        UserDaoImpl.class,
        ServiceFactory.class,
        CassandraOperationImpl.class
})
@PowerMockIgnore({"javax.management.*"})
public class UserMergeActorTest {
    private static final Props props = Props.create(UserMergeActor.class);
    private static ActorSystem system = ActorSystem.create("system");
    public static UserServiceImpl userService;
    public static UserDaoImpl userDao;
    public static CassandraOperationImpl cassandraOperation;

    @Before
    public void beforeEachTest() {
        PowerMockito.mockStatic(UserServiceImpl.class);
        PowerMockito.mockStatic(UserDaoImpl.class);
        userService = mock(UserServiceImpl.class);
        userDao = mock(UserDaoImpl.class);
        when(UserServiceImpl.getInstance()).thenReturn(userService);
        when(UserDaoImpl.getInstance()).thenReturn(userDao);
        cassandraOperation = mock(CassandraOperationImpl.class);

    }

    @Test
    public void testMergeUserIsAlreadyDeleted() {
        when(userService.getUserById(Mockito.anyString())).thenReturn(getUserDetails(true));
        boolean result =
                testScenario(
                        getRequest(ActorOperations.MERGE_USER),
                        ResponseCode.mergeeIdNotExists);
        assertTrue(result);
    }

    @Test
    public void testValidMergeUser() {
        when(userService.getUserById(Mockito.anyString())).thenReturn(getUserDetails(false));
        when(userDao.updateUser(Mockito.anyMap())).thenReturn(getSuccessResponse());
        boolean result =
                testScenario(
                        getRequest(ActorOperations.MERGE_USER),
                        null);
        assertTrue(result);
    }

    public static Response getSuccessResponse() {
        Response response = new Response();
        response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
        return response;
    }

    private User getUserDetails(boolean b) {
        User user = new User();
        user.setIsDeleted(b);
        return user;
    }

    public boolean testScenario(Request reqObj, ResponseCode errorCode) {

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        subject.tell(reqObj, probe.getRef());

        if (errorCode == null) {
            Response res = probe.expectMsgClass(duration("10 second"), Response.class);
            return null != res && res.getResponseCode() == ResponseCode.OK;
        } else {
            ProjectCommonException res =
                    probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
            return res.getCode().equals(errorCode.getErrorCode())
                    || res.getResponseCode() == errorCode.getResponseCode();
        }
    }

    Request getRequest(ActorOperations actorOperation) {
        Request reqObj = new Request();
        Map reqMap = new HashMap<>();
        reqMap.put(JsonKey.FROM_ACCOUNT_ID, "anyUserId");
        reqMap.put(JsonKey.TO_ACCOUNT_ID, "anyUserId");
        reqObj.setRequest(reqMap);
        reqObj.setOperation(actorOperation.getValue());
        return reqObj;
    }

}
