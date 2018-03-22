package org.sunbird.badge.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.badge.BadgeOperations;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseMessage;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class BadgeNotifierTest {

    private static ActorSystem system;
    private static TestKit probe;
    private static ActorRef actor;

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        probe = new TestKit(system);
        actor = system.actorOf(Props.create(BadgeNotifier.class));
    }

    @Test
    public void invalidOperation() {
        Request request = new Request();
        request.setOperation("invalidOperation");
        actor.tell(request, probe.getRef());
        ProjectCommonException ex = probe.expectMsgClass(ProjectCommonException.class);
        Assert.assertTrue(ResponseMessage.Message.INVALID_OPERATION_NAME.equals(ex.getMessage()));
        Assert.assertTrue(ResponseMessage.Key.INVALID_OPERATION_NAME.equals(ex.getCode()));
    }

    @Test
    public void assignBadgeWithoutObjectType() {
        Request request = new Request();
        request.setOperation(BadgeOperations.assignBadgeMessage.name());
        Map<String, Object> data = new HashMap<String, Object>();
        request.setRequest(data);
        data.put(JsonKey.ID, "test_content");
        data.put(BadgingJsonKey.BADGE_ASSERTION, new HashMap<>());
        actor.tell(request, probe.getRef());
        Response res = probe.expectMsgClass(Response.class);
        Assert.assertTrue(null != res);
    }

    @Test
    public void assignBadgeWithoutId() {
        Request request = new Request();
        request.setOperation(BadgeOperations.assignBadgeMessage.name());
        Map<String, Object> data = new HashMap<String, Object>();
        request.setRequest(data);
        data.put(JsonKey.OBJECT_TYPE, "Content");
        data.put(BadgingJsonKey.BADGE_ASSERTION, new HashMap<>());
        actor.tell(request, probe.getRef());
        ProjectCommonException ex = probe.expectMsgClass(ProjectCommonException.class);
        Assert.assertTrue("Please provide content id.".equals(ex.getMessage()));
    }

    @Test
    public void assignBadgeWithout() {
        Request request = new Request();
        request.setOperation(BadgeOperations.assignBadgeMessage.name());
        Map<String, Object> data = new HashMap<String, Object>();
        request.setRequest(data);
        data.put(JsonKey.OBJECT_TYPE, "Content");
        data.put(JsonKey.ID, "test_content");
        actor.tell(request, probe.getRef());
        ProjectCommonException ex = probe.expectMsgClass(ProjectCommonException.class);
        Assert.assertTrue("Please provide badge details.".equals(ex.getMessage()));
    }
}
