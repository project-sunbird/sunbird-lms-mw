package org.sunbird.learner.actors.geolocation;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;

/**
 * Created by arvind on 3/11/17.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GeoLocationManagementActorTest {

	private static ActorSystem system;
	private static final Props props = Props.create(GeoLocationManagementActor.class);
	private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
	private static Util.DbInfo geoLocationDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
	private static Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
	private static final String orgId = "hhjcjr79fw4p89";
	private static final String type = "husvej";
	private static final String userId = "vcurc633r8911";
	private static List<Map<String, Object>> createResponse;
	private static String id;

	@BeforeClass
	public static void setUp() {

		Util.checkCassandraDbConnections(JsonKey.SUNBIRD);
		system = ActorSystem.create("system");

		Map<String, Object> orgMap = new HashMap<String, Object>();
		orgMap.put(JsonKey.ID, orgId);
		cassandraOperation.insertRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgMap);

	}

	@Test
	public void acreateGeoLocationTest() {

		List<Map<String, Object>> dataList = new ArrayList<>();

		Map<String, Object> dataMap = new HashMap<>();
		dataMap.put(JsonKey.LOCATION, "location");
		dataMap.put(JsonKey.TYPE, type);

		dataList.add(dataMap);

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.REQUESTED_BY, userId);
		actorMessage.setOperation(ActorOperations.CREATE_GEO_LOCATION.getValue());

		actorMessage.getRequest().put(JsonKey.DATA, dataList);
		actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID, orgId);

		subject.tell(actorMessage, probe.getRef());
		Response res = probe.expectMsgClass(duration("100 second"), Response.class);
		createResponse = (List<Map<String, Object>>) res.getResult().get(JsonKey.RESPONSE);
		if (createResponse != null && createResponse.size() > 0) {
			id = (String) createResponse.get(0).get(JsonKey.ID);
			createResponse.remove(createResponse.get(0));
		}
		Assert.assertTrue(null != id);
	}

	@Test
	public void createGeoLocationTestWithNullOrgId() {

		List<Map<String, Object>> dataList = new ArrayList<>();

		Map<String, Object> dataMap = new HashMap<>();
		dataMap.put(JsonKey.LOCATION, "location");
		dataMap.put(JsonKey.TYPE, type);

		dataList.add(dataMap);

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.REQUESTED_BY, userId);
		actorMessage.setOperation(ActorOperations.CREATE_GEO_LOCATION.getValue());

		actorMessage.getRequest().put(JsonKey.DATA, dataList);
		actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID, null);

		subject.tell(actorMessage, probe.getRef());
		ProjectCommonException exc = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
		Assert.assertTrue(null != exc);

	}

	@Test
	public void createGeoLocationTestWithInvalidOrgId() {

		List<Map<String, Object>> dataList = new ArrayList<>();

		Map<String, Object> dataMap = new HashMap<>();
		dataMap.put(JsonKey.LOCATION, "location");
		dataMap.put(JsonKey.TYPE, type);

		dataList.add(dataMap);

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.REQUESTED_BY, userId);
		actorMessage.setOperation(ActorOperations.CREATE_GEO_LOCATION.getValue());

		actorMessage.getRequest().put(JsonKey.DATA, dataList);
		actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID, orgId + "jfjrrou");

		subject.tell(actorMessage, probe.getRef());
		ProjectCommonException exc = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
		Assert.assertTrue(null != exc);
	}

	@Test
	public void createGeoLocationTestWithInvalidData() {

		List<Map<String, Object>> dataList = new ArrayList<>();

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.REQUESTED_BY, userId);
		actorMessage.setOperation(ActorOperations.CREATE_GEO_LOCATION.getValue());

		actorMessage.getRequest().put(JsonKey.DATA, dataList);
		actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID, orgId);

		subject.tell(actorMessage, probe.getRef());
		ProjectCommonException exc = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
		Assert.assertTrue(null != exc);
	}

	@Test
	public void getGeoLocationTestyOrgId() {

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.REQUESTED_BY, userId);
		actorMessage.getRequest().put(JsonKey.TYPE, JsonKey.ORGANISATION);
		actorMessage.getRequest().put(JsonKey.ID, orgId);
		actorMessage.setOperation(ActorOperations.GET_GEO_LOCATION.getValue());

		actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID, orgId);

		subject.tell(actorMessage, probe.getRef());
		Response res = probe.expectMsgClass(duration("100 second"), Response.class);
		Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
	}

	@Test
	public void getGeoLocationTestyLocationId() {

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.REQUESTED_BY, userId);
		actorMessage.getRequest().put(JsonKey.TYPE, JsonKey.LOCATION);
		actorMessage.getRequest().put(JsonKey.ID, id);
		actorMessage.setOperation(ActorOperations.GET_GEO_LOCATION.getValue());

		actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID, orgId);

		subject.tell(actorMessage, probe.getRef());
		Response res = probe.expectMsgClass(duration("100 second"), Response.class);
		Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
	}

	@Test
	public void getGeoLocationTestWithNullType() {

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.REQUESTED_BY, userId);
		actorMessage.getRequest().put(JsonKey.TYPE, null);
		actorMessage.getRequest().put(JsonKey.ID, orgId);
		actorMessage.setOperation(ActorOperations.GET_GEO_LOCATION.getValue());

		actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID, orgId);

		subject.tell(actorMessage, probe.getRef());
		ProjectCommonException exc = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
		Assert.assertTrue(null != exc);
	}

	@Test
	public void getGeoLocationTestWithInvalidType() {

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.REQUESTED_BY, userId);
		actorMessage.getRequest().put(JsonKey.TYPE, "Invalid type");
		actorMessage.getRequest().put(JsonKey.ID, orgId);
		actorMessage.setOperation(ActorOperations.GET_GEO_LOCATION.getValue());

		actorMessage.getRequest().put(JsonKey.ROOT_ORG_ID, orgId);

		subject.tell(actorMessage, probe.getRef());
		ProjectCommonException exc = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
		Assert.assertTrue(null != exc);
	}

	@Test
	public void updateGeoLocationTest() {

		if (null != id) {

			TestKit probe = new TestKit(system);
			ActorRef subject = system.actorOf(props);
			Request actorMessage = new Request();

			actorMessage.getRequest().put(JsonKey.REQUESTED_BY, userId);
			actorMessage.getRequest().put(JsonKey.LOCATION, "updated location");
			actorMessage.getRequest().put(JsonKey.TYPE, type);
			actorMessage.getRequest().put(JsonKey.LOCATION_ID, id);
			actorMessage.setOperation(ActorOperations.UPDATE_GEO_LOCATION.getValue());

			subject.tell(actorMessage, probe.getRef());
			Response res = probe.expectMsgClass(duration("100 second"), Response.class);
			Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
		}

	}

	@Test
	public void updateGeoLocationTestWithNullId() {

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.REQUESTED_BY, userId);
		actorMessage.getRequest().put(JsonKey.LOCATION, "updated location");
		actorMessage.getRequest().put(JsonKey.TYPE, type);
		actorMessage.getRequest().put(JsonKey.LOCATION_ID, null);
		actorMessage.setOperation(ActorOperations.UPDATE_GEO_LOCATION.getValue());

		subject.tell(actorMessage, probe.getRef());
		ProjectCommonException exc = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
		Assert.assertTrue(null != exc);

	}

	@Test
	public void updateGeoLocationTestWithInvalidId() {

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.REQUESTED_BY, userId);
		actorMessage.getRequest().put(JsonKey.LOCATION, "updated location");
		actorMessage.getRequest().put(JsonKey.TYPE, type);
		actorMessage.getRequest().put(JsonKey.LOCATION_ID, id + "-invalid");
		actorMessage.setOperation(ActorOperations.UPDATE_GEO_LOCATION.getValue());

		subject.tell(actorMessage, probe.getRef());
		ProjectCommonException exc = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
		Assert.assertTrue(null != exc);

	}

	@Test
	public void zdeleteGeoLocationTest() {

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.LOCATION_ID, id);
		actorMessage.setOperation(ActorOperations.DELETE_GEO_LOCATION.getValue());

		subject.tell(actorMessage, probe.getRef());
		Response res = probe.expectMsgClass(duration("100 second"), Response.class);
		Assert.assertTrue(null != res.get(JsonKey.RESPONSE));
	}

	@Test
	public void deleteGeoLocationTestWithNullId() {

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.LOCATION_ID, null);
		actorMessage.setOperation(ActorOperations.DELETE_GEO_LOCATION.getValue());

		subject.tell(actorMessage, probe.getRef());
		ProjectCommonException exc = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
		Assert.assertTrue(null != exc);
	}

	@Test
	public void geoLocationTestWithInvalidOperation() {

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();

		actorMessage.getRequest().put(JsonKey.LOCATION_ID, null);
		actorMessage.setOperation("invalid operation");

		subject.tell(actorMessage, probe.getRef());
		ProjectCommonException exc = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
		Assert.assertTrue(null != exc);
	}

	@Test
	public void geoLocationTestWithInvalidObjectType() {

		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);

		subject.tell("Invalid Request", probe.getRef());
		ProjectCommonException exc = probe.expectMsgClass(duration("100 second"), ProjectCommonException.class);
		Assert.assertTrue(null != exc);
	}

	@Test
	public void getUserCount() {
		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();
		List<Object> list = new ArrayList<>();
		list.add("locId1");
		actorMessage.getRequest().put(JsonKey.LOCATION_IDS, list);
		actorMessage.setOperation(ActorOperations.GET_USER_COUNT.getValue());

		subject.tell(actorMessage, probe.getRef());
		Response res = probe.expectMsgClass(duration("100 second"), Response.class);
		List<Map<String, Object>> result = (List<Map<String, Object>>) res.getResult().get(JsonKey.LOCATIONS);
		Map<String, Object> map = result.get(0);
		int count = (int) map.get(JsonKey.USER_COUNT);
		assertEquals(0, count);
	}

	@Test
	public void getUserCount2() {
		TestKit probe = new TestKit(system);
		ActorRef subject = system.actorOf(props);
		Request actorMessage = new Request();
		List<Object> list = new ArrayList<>();
		list.add(id);
		actorMessage.getRequest().put(JsonKey.LOCATION_IDS, list);
		actorMessage.setOperation(ActorOperations.GET_USER_COUNT.getValue());

		subject.tell(actorMessage, probe.getRef());
		Response res = probe.expectMsgClass(duration("100 second"), Response.class);
		List<Map<String, Object>> result = (List<Map<String, Object>>) res.getResult().get(JsonKey.LOCATIONS);
		Map<String, Object> map = result.get(0);
		int count = (int) map.get(JsonKey.USER_COUNT);
		assertEquals(0, count);
	}

	@AfterClass
	public static void destroy() {

		cassandraOperation.deleteRecord(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), orgId);
		for (Map<String, Object> m : createResponse) {

			String id = (String) m.get(JsonKey.ID);
			if (!ProjectUtil.isStringNullOREmpty(id)) {
				cassandraOperation.deleteRecord(geoLocationDbInfo.getKeySpace(), geoLocationDbInfo.getTableName(), id);
			}

		}

	}

}
