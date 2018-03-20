package org.sunbird.badge.actors;

import static akka.testkit.JavaTestKit.duration;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.badge.service.impl.BadgrServiceImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingActorOperations;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import scala.concurrent.duration.FiniteDuration;

@RunWith(PowerMockRunner.class)
@PrepareForTest(BadgeClassActor.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class BadgeClassActorTest {
    private static final FiniteDuration ACTOR_MAX_WAIT_DURATION = duration("100 second");

    private ActorSystem system;
    private Props props;

    private TestKit probe;
    private ActorRef subject;

    private Request actorMessage;

    private BadgrServiceImpl mockBadgingService;
    private ProjectCommonException resourceNotFoundException;

    @Before
    public void setup() {
        system = ActorSystem.create("system");
        probe = new TestKit(system);

        mockBadgingService = PowerMockito.mock(BadgrServiceImpl.class);

        props = Props.create(BadgeClassActor.class, mockBadgingService);
        subject = system.actorOf(props);

        actorMessage = new Request();

        ResponseCode error = ResponseCode.resourceNotFound;
        resourceNotFoundException = new ProjectCommonException(error.getErrorCode(), error.getErrorMessage(), error.getResponseCode());

    }

    @Test
    public void testCreateBadgeClassSuccess() {
        PowerMockito.when(mockBadgingService.createBadgeClass(actorMessage)).thenReturn(new Response());

        actorMessage.setOperation(BadgingActorOperations.CREATE_BADGE_CLASS.getValue());

        subject.tell(actorMessage, probe.getRef());

        Response response = probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, Response.class);
        Assert.assertTrue(null != response);
    }

    @Test
    public void testCreateBadgeClassFailure() {
        PowerMockito.when(mockBadgingService.createBadgeClass(actorMessage)).thenThrow(resourceNotFoundException);

        actorMessage.setOperation(BadgingActorOperations.CREATE_BADGE_CLASS.getValue());

        subject.tell(actorMessage, probe.getRef());

        ProjectCommonException exception = probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, ProjectCommonException.class);
        Assert.assertTrue(null != exception);
    }

    @Test
    public void testGetBadgeClassSuccess() {
        PowerMockito.when(mockBadgingService.getBadgeClassDetails(actorMessage)).thenReturn(new Response());

        actorMessage.setOperation(BadgingActorOperations.GET_BADGE_CLASS.getValue());

        subject.tell(actorMessage, probe.getRef());

        Response response = probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, Response.class);
        Assert.assertTrue(null != response);
    }

    @Test
    public void testGetBadgeClassFailure() {
        PowerMockito.when(mockBadgingService.getBadgeClassDetails(actorMessage)).thenThrow(resourceNotFoundException);

        actorMessage.setOperation(BadgingActorOperations.GET_BADGE_CLASS.getValue());

        subject.tell(actorMessage, probe.getRef());

        ProjectCommonException exception = probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, ProjectCommonException.class);
        Assert.assertTrue(null != exception);
    }

    @Test
    public void testListBadgeClassSuccess() {
        PowerMockito.when(mockBadgingService.getBadgeClassList(actorMessage)).thenReturn(new Response());

        actorMessage.setOperation(BadgingActorOperations.LIST_BADGE_CLASS.getValue());

        subject.tell(actorMessage, probe.getRef());

        Response response = probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, Response.class);
        Assert.assertTrue(null != response);
    }

    @Test
    public void testListBadgeClassFailure() {
        PowerMockito.when(mockBadgingService.getBadgeClassList(actorMessage)).thenThrow(resourceNotFoundException);

        actorMessage.setOperation(BadgingActorOperations.LIST_BADGE_CLASS.getValue());

        subject.tell(actorMessage, probe.getRef());

        ProjectCommonException exception = probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, ProjectCommonException.class);
        Assert.assertTrue(null != exception);
    }

    @Test
    public void testDeleteBadgeClassSuccess() {
        PowerMockito.when(mockBadgingService.removeBadgeClass(actorMessage)).thenReturn(new Response());

        actorMessage.setOperation(BadgingActorOperations.DELETE_BADGE_CLASS.getValue());

        subject.tell(actorMessage, probe.getRef());

        Response response = probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, Response.class);
        Assert.assertTrue(null != response);
    }

    @Test
    public void testDeleteBadgeClassFailure() {
        PowerMockito.when(mockBadgingService.removeBadgeClass(actorMessage)).thenThrow(resourceNotFoundException);

        actorMessage.setOperation(BadgingActorOperations.DELETE_BADGE_CLASS.getValue());

        subject.tell(actorMessage, probe.getRef());

        ProjectCommonException exception = probe.expectMsgClass(ACTOR_MAX_WAIT_DURATION, ProjectCommonException.class);
        Assert.assertTrue(null != exception);
    }
}
