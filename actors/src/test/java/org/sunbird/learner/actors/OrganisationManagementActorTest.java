package org.sunbird.learner.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.learner.util.Util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author arvind.
 */
public class OrganisationManagementActorTest {


    static ActorSystem system;
    final static Props props = Props.create(OrganisationManagementActor.class);

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("system");
        Util.checkCassandraDbConnections();
    }

    @Test
    public void testGetOrg(){

        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation(ActorOperations.GET_ORG_DETAILS.getValue());
        HashMap<String, Object> innerMap = new HashMap<>();
        Map<String , Object> orgMap = new HashMap<String , Object>();
        orgMap.put(JsonKey.ORGANISATION_ID , "CBSE");
        innerMap.put(JsonKey.ORGANISATION , orgMap);

        reqObj.setRequest(innerMap);
        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(Response.class);

    }

    @Test
    public void testInvalidOperation(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        Request reqObj = new Request();
        reqObj.setOperation("INVALID_OPERATION");

        subject.tell(reqObj, probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);
    }

    @Test
    public void testInvalidMessageType(){
        TestKit probe = new TestKit(system);
        ActorRef subject = system.actorOf(props);

        subject.tell(new String("Invelid Type"), probe.getRef());
        probe.expectMsgClass(ProjectCommonException.class);
    }

}
