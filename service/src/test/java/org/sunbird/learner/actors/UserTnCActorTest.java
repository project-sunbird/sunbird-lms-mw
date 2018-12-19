package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
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
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.actorutil.systemsettings.SystemSettingClient;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.tac.UserTnCActor;

import java.util.HashMap;

import static akka.testkit.JavaTestKit.duration;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class, SystemSettingClientImpl.class, RequestRouter.class})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*", "javax.net.ssl.*", "javax.security.*"})
public class UserTnCActorTest {
    private static ActorSystem system;
    private static final Props props = Props.create(UserTnCActor.class);
    private static final CassandraOperation cassandraOperation = mock(CassandraOperationImpl.class);;
    public static SystemSettingClient systemSettingClient;

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
    }
    @Before
    public void beforeEachTest() {
        PowerMockito.mockStatic(ServiceFactory.class);
        PowerMockito.mockStatic(SystemSettingClientImpl.class);
        systemSettingClient = mock(SystemSettingClientImpl.class);
        when(SystemSettingClientImpl.getInstance()).thenReturn(systemSettingClient);

        ActorRef actorRef = mock(ActorRef.class);

        when(systemSettingClient.getSystemSettingByFieldAndKey(eq(actorRef), Mockito.anyString(),Mockito.anyString(),Mockito.anyObject()))
                .thenReturn("value");
        when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);

    }
    @Test
    public void testAcceptUserTncFailure() {
        ProjectCommonException exception = performUserTnCAcceptFailureTest();
        Assert.assertTrue(null != exception);
    }

    private ProjectCommonException performUserTnCAcceptFailureTest() {
//        mockTncSystemSetting();
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.USER_TNC_ACCEPT.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        innerMap.put(JsonKey.VERSION,"randomValue");
        reqObj.setRequest(innerMap);
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);
        subject.tell(reqObj, probe.getRef());
        ProjectCommonException exception = probe.expectMsgClass(duration("1000 second"), ProjectCommonException.class);

        return exception;
    }

//    private void mockTncSystemSetting() {
//        when(systemSettingClient.getSystemSettingByFieldAndKey(
//                (ActorRef) Mockito.any(), Mockito.anyString(),Mockito.anyString(),Mockito.any()))
//                .thenReturn("value");
//    }



    private String getSystemSettingResponse() {
//        Response systemSettingResponse = new Response();
//        Map<String,Object> innerMap = new HashMap<>();
//        innerMap.put(JsonKey.KEY,Mockito.anyString());
//        innerMap.put(JsonKey.FIELD,Mockito.anyString());
//        innerMap.put(JsonKey.VALUE,new HashMap<String,Object>().put(JsonKey.LATEST_VERSION,"someVersion"));
//        systemSettingResponse.put(JsonKey.RESPONSE,innerMap);

        return "someVersion";
    }
}
