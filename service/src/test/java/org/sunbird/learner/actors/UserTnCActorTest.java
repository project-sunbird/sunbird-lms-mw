package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.Assert;
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
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.coursebatch.CourseBatchManagementActor;

import java.util.HashMap;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceFactory.class})
@PowerMockIgnore("javax.management.*")
public class UserTnCActorTest {
    private TestKit probe;
    private ActorRef subject;
    private static CassandraOperationImpl mockCassandraOperation;

    @Before
    public void setUp() {
        mockCassandraOperation = mock(CassandraOperationImpl.class);

        ActorSystem system = ActorSystem.create("system");
        probe = new TestKit(system);

        Props props = Props.create(UserTnCActorTest.class, mockCassandraOperation);
        subject = system.actorOf(props);

        PowerMockito.mockStatic(ServiceFactory.class);
        when(ServiceFactory.getInstance()).thenReturn(mockCassandraOperation);
    }

    @Test
    public void testAcceptTnCFailureWithInvalidVersionTest() {

    }

    @Test
    public void testAcceptTnCSuccessWithInvalidVersionTest() {

    }

    private ProjectCommonException performUserTnCAcceptFailureTest(
            Response tncResponse) {
        when(mockCassandraOperation.getRecordById(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(tncResponse);
        when(mockCassandraOperation.getRecordById(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(tncResponse);
        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.USER_TNC_ACCEPT.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        subject.tell(reqObj, probe.getRef());

        ProjectCommonException exception =
                probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
        return exception;
    }
}
