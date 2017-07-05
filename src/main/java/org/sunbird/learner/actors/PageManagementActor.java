package org.sunbird.learner.actors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LogHelper;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.UntypedAbstractActor;

/**
 * This actor will handle page management operation .
 *
 * @author Amit Kumar
 */
public class PageManagementActor extends UntypedAbstractActor {
	private LogHelper logger = LogHelper.getInstance(PageManagementActor.class.getName());

	private static Map<String, String> headers = new HashMap<String, String>();
	static {
		headers.put("content-type", "application/json");
		headers.put("accept", "application/json");
		headers.put("user-id", "mahesh");
	}
	private CassandraOperation cassandraOperation = new CassandraOperationImpl();
	Util.DbInfo pageDbInfo = Util.dbInfoMap.get(JsonKey.PAGE_MGMT_DB);
	Util.DbInfo sectionDbInfo = Util.dbInfoMap.get(JsonKey.SECTION_MGMT_DB);
	Util.DbInfo pageSectionDbInfo = Util.dbInfoMap.get(JsonKey.PAGE_SECTION_DB);
	Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
	@Override
	public void onReceive(Object message) throws Throwable {
		if (message instanceof Request) {
			logger.info("PageManagementActor onReceive called");
			Request actorMessage = (Request) message;
			if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.CREATE_PAGE.getValue())) {
				createPage(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.UPDATE_PAGE.getValue())) {
				updatePage(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_PAGE_SETTING.getValue())) {
				getPageSetting(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_PAGE_SETTINGS.getValue())) {
				getPageSettings(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_PAGE_DATA.getValue())) {
				getPageData(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.CREATE_SECTION.getValue())) {
				createPageSection(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.UPDATE_SECTION.getValue())) {
				updatePageSection(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_SECTION.getValue())) {
				getSection(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_ALL_SECTION.getValue())) {
				getAllSections(actorMessage);
			} else {
				logger.info("UNSUPPORTED OPERATION");
				ProjectCommonException exception = new ProjectCommonException(
						ResponseCode.invalidOperationName.getErrorCode(),
						ResponseCode.invalidOperationName.getErrorMessage(),
						ResponseCode.CLIENT_ERROR.getResponseCode());
				sender().tell(exception, self());
			}
		} else {
			// Throw exception as message body
			logger.info("UNSUPPORTED MESSAGE");
			ProjectCommonException exception = new ProjectCommonException(
					ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(),
					ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
		}

	}

	private void getAllSections(Request actorMessage) {
		Response response = null;
		response = cassandraOperation.getAllRecords(sectionDbInfo.getKeySpace(), sectionDbInfo.getTableName());
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> result = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
		for (Map<String, Object> map : result) {
			removeUnwantedData(map,"");
		}
		Response sectionMap = new Response();
		sectionMap.put(JsonKey.SECTIONS, response.get(JsonKey.RESPONSE));
		sender().tell(response, self());

	}

	private void getSection(Request actorMessage) {
		Response response = null;
		Map<String, Object> req = actorMessage.getRequest();
		String sectionId = (String) req.get(JsonKey.ID);
		response = cassandraOperation.getRecordById(sectionDbInfo.getKeySpace(), sectionDbInfo.getTableName(),
				sectionId);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> result = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
		if(!(result.isEmpty())) {
			Map<String, Object> map = result.get(0);
			removeUnwantedData(map,"");
			Response section = new Response();
			section.put(JsonKey.SECTION, response.get(JsonKey.RESPONSE));
		}
		sender().tell(response, self());
	}

	private void updatePageSection(Request actorMessage) {
		Map<String, Object> req = actorMessage.getRequest();
		@SuppressWarnings("unchecked")
		Map<String, Object> sectionMap = (Map<String, Object>) req.get(JsonKey.SECTION);
		if (null != sectionMap.get(JsonKey.SEARCH_QUERY)) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				sectionMap.put(JsonKey.SEARCH_QUERY, mapper.writeValueAsString(sectionMap.get(JsonKey.SEARCH_QUERY)));
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		if (null != sectionMap.get(JsonKey.SECTION_DISPLAY)) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				sectionMap.put(JsonKey.SECTION_DISPLAY, mapper.writeValueAsString(sectionMap.get(JsonKey.SECTION_DISPLAY)));
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		sectionMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
		Response response = cassandraOperation.updateRecord(sectionDbInfo.getKeySpace(), sectionDbInfo.getTableName(),
				sectionMap);
		sender().tell(response, self());
	}

	private void createPageSection(Request actorMessage) {
		Map<String, Object> req = actorMessage.getRequest();
		@SuppressWarnings("unchecked")
		Map<String, Object> sectionMap = (Map<String, Object>) req.get(JsonKey.SECTION);
		String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
		if (null != sectionMap.get(JsonKey.SEARCH_QUERY)) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				sectionMap.put(JsonKey.SEARCH_QUERY, mapper.writeValueAsString(sectionMap.get(JsonKey.SEARCH_QUERY)));
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		if (null != sectionMap.get(JsonKey.SECTION_DISPLAY)) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				sectionMap.put(JsonKey.SECTION_DISPLAY,mapper.writeValueAsString(sectionMap.get(JsonKey.SECTION_DISPLAY)));
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		sectionMap.put(JsonKey.ID, uniqueId);
		sectionMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
		sectionMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
		Response response = cassandraOperation.insertRecord(sectionDbInfo.getKeySpace(), sectionDbInfo.getTableName(),
				sectionMap);
		sender().tell(response, self());
	}

	@SuppressWarnings("unchecked")
	private void getPageData(Request actorMessage) {
		String sectionQuery = null;
		Response response = null;
		Map<String, Object> req = actorMessage.getRequest();
		String pageName = (String) req.get(JsonKey.ID);
		//String userId = (String) req.get(JsonKey.USER_ID);
		String source = (String) req.get(JsonKey.SOURCE);
		String orgCode = (String) req.get(JsonKey.ORG_CODE);
		List<Map<String, Object>> result = null;
		try{
			response = cassandraOperation.getRecordsByProperty(orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), JsonKey.ORG_CODE, orgCode);
			result = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		
		Map<String, Object> map = null;
		String orgId = "NA";
		if(null != result && result.size() > 0){
			map = result.get(0);
			orgId = (String) map.get(JsonKey.ID);
		}
		
		/**
		 * based on source fetch the list of section from page object to display on page
		 * if source is web then portal map contains the list of section id to display
		 * if source is app then app map contains the information
		 */
		Map<String, Object> pageMap = DataCacheHandler.pageMap.get(orgId+":"+pageName);
		/**
		 * if requested page for this organisation is not found,
		 * return default NTP page
		 */
		if(null == pageMap){
			pageMap = DataCacheHandler.pageMap.get("NA"+":"+pageName);
		}
		if (source.equalsIgnoreCase(ProjectUtil.Source.WEB.getValue())) {
			if(null != pageMap && null != pageMap.get(JsonKey.PORTAL_MAP)){
				sectionQuery = (String) pageMap.get(JsonKey.PORTAL_MAP);
			}
		} else {
			if(null != pageMap && null != pageMap.get(JsonKey.APP_MAP)){
				sectionQuery = (String) pageMap.get(JsonKey.APP_MAP);
			}
		}
		ObjectMapper mapper = new ObjectMapper();
		List<Map<String, Object>> sections = new ArrayList<Map<String, Object>>();
		try {
			Object[] arr = mapper.readValue(sectionQuery, Object[].class);
			for(Object obj : arr){
				Map<String,Object>  secMap = (Map<String, Object>) obj ;
				Map<String,Object>  section = new HashMap<>();
				section.putAll(secMap);
				List<Map<String,Object>>  subSectionList = (List<Map<String, Object>>) secMap.get(JsonKey.SUB_SECTIONS);
				List<Map<String, Object>> innerSectionList = new ArrayList<>();
				for(Map<String,Object> sec : subSectionList){
					Map<String, Object> subSectionData = DataCacheHandler.sectionMap.get((String)sec.get(JsonKey.ID));
					Map<String, Object> subSectionMap = null;
					
					subSectionMap = new HashMap<>();
					subSectionMap.putAll(subSectionData);
					/**
					 * based on section data type fetch the information 
					 * if data type is course then course data will be fetched from NTP ES server
					 * if data type is content then content data will be fethced from EkStep server
					 */
					if (((String) subSectionData.get(JsonKey.SECTION_DATA_TYPE))
							.equals(ProjectUtil.SectionDataType.content.getTypeName())) {
						subSectionMap = new HashMap<>();
						subSectionMap.putAll(subSectionData);
						getContentData(subSectionMap);
					} else {
						subSectionMap = new HashMap<>();
						subSectionMap.putAll(subSectionData);
						getCourseData(subSectionMap);
					}
					//getContentData(subSectionMap);
					
					subSectionMap.put(JsonKey.POSITION, sec.get(JsonKey.POSITION));
					removeUnwantedData(subSectionMap,"getPageData");
					
					if(null != subSectionData && subSectionData.size()>0){
						innerSectionList.add(subSectionMap);
					}
				}
				section.put(JsonKey.SUB_SECTIONS, innerSectionList);
				sections.add(section);
			}
			
		} catch (JsonParseException e) {
			logger.error(e.getMessage(), e);
		} catch (JsonMappingException e) {
			logger.error(e.getMessage(), e);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} catch (Exception e){
			logger.error(e.getMessage(), e);
		}
	
		Map<String, Object> responseMap = new HashMap<>();
		responseMap.put(JsonKey.NAME, pageMap.get(JsonKey.NAME));
		responseMap.put(JsonKey.ID, pageMap.get(JsonKey.ID));
		responseMap.put(JsonKey.SECTIONS, sections);

		Response pageResponse = new Response();
		pageResponse.put(JsonKey.RESPONSE, responseMap);
		sender().tell(pageResponse, self());
	}

	@SuppressWarnings("unchecked")
	private void getPageSetting(Request actorMessage) {

		Map<String, Object> req = actorMessage.getRequest();
		String pageName = (String) req.get(JsonKey.ID);
		Response response = cassandraOperation.getRecordsByProperty(pageDbInfo.getKeySpace(), pageDbInfo.getTableName(),
				JsonKey.PAGE_NAME, pageName);
		List<Map<String, Object>> result = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
		if(!(result.isEmpty())) {
			Map<String, Object> pageDO = result.get(0);
			Map<String, Object> responseMap = getPageSetting(pageDO);
			response.getResult().put(JsonKey.PAGE, responseMap);
			response.getResult().remove(JsonKey.RESPONSE);
		}

		sender().tell(response, self());
	}
	
	@SuppressWarnings("unchecked")
	private void getPageSettings(Request actorMessage) {

		Response response = cassandraOperation.getAllRecords(pageDbInfo.getKeySpace(), pageDbInfo.getTableName());
		List<Map<String, Object>> result = (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);

		List<Map<String, Object>> pageList = new ArrayList<Map<String, Object>>();

		for (Map<String, Object> pageDO : result) {
			Map<String, Object> responseMap = getPageSetting(pageDO);
			pageList.add(responseMap);
		}

		response.getResult().put(JsonKey.PAGE, pageList);
		response.getResult().remove(JsonKey.RESPONSE);
		sender().tell(response, self());

	}

	@SuppressWarnings("unchecked")
	private void updatePage(Request actorMessage) {
		Map<String, Object> req = actorMessage.getRequest();
		Map<String, Object> pageMap = (Map<String, Object>) req.get(JsonKey.PAGE);
		pageMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
		if (null != pageMap.get(JsonKey.PORTAL_MAP)) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				pageMap.put(JsonKey.PORTAL_MAP, mapper.writeValueAsString(pageMap.get(JsonKey.PORTAL_MAP)));
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		if (null != pageMap.get(JsonKey.APP_MAP)) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				pageMap.put(JsonKey.APP_MAP, mapper.writeValueAsString(pageMap.get(JsonKey.APP_MAP)));
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		Response response = cassandraOperation.updateRecord(pageDbInfo.getKeySpace(), pageDbInfo.getTableName(),
				pageMap);
		sender().tell(response, self());
	}

	@SuppressWarnings("unchecked")
	private void createPage(Request actorMessage) {
		Map<String, Object> req = actorMessage.getRequest();
		Map<String, Object> pageMap = (Map<String, Object>) req.get(JsonKey.PAGE);
		String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
		pageMap.put(JsonKey.ID, uniqueId);
		pageMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
		if (null != pageMap.get(JsonKey.PORTAL_MAP)) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				pageMap.put(JsonKey.PORTAL_MAP, mapper.writeValueAsString(pageMap.get(JsonKey.PORTAL_MAP)));
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		if (null != pageMap.get(JsonKey.APP_MAP)) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				pageMap.put(JsonKey.APP_MAP, mapper.writeValueAsString(pageMap.get(JsonKey.APP_MAP)));
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		Response response = cassandraOperation.insertRecord(pageDbInfo.getKeySpace(), pageDbInfo.getTableName(),
				pageMap);
		sender().tell(response, self());
	}


	private void getContentData(Map<String, Object> section) {
		String response = "";
		JSONObject data;
		JSONObject jObject;
		ObjectMapper mapper = new ObjectMapper();
		try {
			response = HttpUtil.sendPostRequest(PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTNET_SEARCH_URL),
					(String) section.get(JsonKey.SEARCH_QUERY), headers);
			jObject = new JSONObject(response);
			data = jObject.getJSONObject(JsonKey.RESULT);
			JSONArray contentArray = data.getJSONArray(JsonKey.CONTENT);
			section.put(JsonKey.CONTENTS, mapper.readValue(contentArray.toString(), Object[].class));
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} catch (JSONException e) {
			logger.error(e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	private void getCourseData(Map<String, Object> section) {
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> result = null;
		Map<String, List<Map<String, Object>>> response = null;
		String searchQuery = (String) section.get(JsonKey.SEARCH_QUERY);
		
		try {
			JSONObject object = new JSONObject(searchQuery);
			JSONObject reqObj = object.getJSONObject(JsonKey.REQUEST);
			result = mapper.readValue(reqObj.toString(), HashMap.class);
		} catch (JsonParseException e) {
			logger.error(e.getMessage(), e);
		} catch (JsonMappingException e) {
			logger.error(e.getMessage(), e);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} catch (JSONException e) {
			logger.error(e.getMessage(), e);
		}
		SearchDTO searchDto = Util.createSearchDto(result);
		response = ElasticSearchUtil.complexSearch(searchDto, ProjectUtil.EsIndex.sunbird.getIndexName(),
				ProjectUtil.EsType.course.getTypeName());
		section.put(JsonKey.CONTENTS, response);

	}
	
	private Map<String, Object> getPageSetting(Map<String, Object> pageDO) {

		Map<String, Object> responseMap = new HashMap<>();
		responseMap.put(JsonKey.NAME, pageDO.get(JsonKey.NAME));
		responseMap.put(JsonKey.ID, pageDO.get(JsonKey.ID));
		
		if (pageDO.containsKey(JsonKey.APP_MAP) && null != pageDO.get(JsonKey.APP_MAP)) {
			responseMap.put(JsonKey.APP_SECTIONS, parseSectionQuery((String)pageDO.get(JsonKey.APP_MAP),pageDO));
		}
		if (pageDO.containsKey(JsonKey.PORTAL_MAP) && null != pageDO.get(JsonKey.PORTAL_MAP)) {
			responseMap.put(JsonKey.PORTAL_SECTIONS, parseSectionQuery((String)pageDO.get(JsonKey.PORTAL_MAP),pageDO));
		}
		return responseMap;
	}

	private void removeUnwantedData(Map<String, Object> map,String from) {
		map.remove(JsonKey.CREATED_DATE);
		map.remove(JsonKey.CREATED_BY);
		map.remove(JsonKey.UPDATED_DATE);
		map.remove(JsonKey.UPDATED_BY);
		if(from.equalsIgnoreCase("getPageData")){
			map.remove(JsonKey.STATUS);
		}
	}

	
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> parseSectionQuery(String sectionDetails, Map<String, Object> pageDO) {
		ObjectMapper mapper = new ObjectMapper();
		List<Map<String, Object>> sections = new ArrayList<Map<String, Object>>();
		try {
			Object[] arr = mapper.readValue(sectionDetails, Object[].class);
			for(Object obj : arr){
				Map<String,Object>  map = (Map<String, Object>) obj ;
				List<Map<String, Object>> subSectionList = new ArrayList<Map<String, Object>>();
				List<Map<String,Object>>  sectionMapList = (List<Map<String, Object>>) map.get(JsonKey.SUB_SECTIONS);
				for(Map<String,Object> sect : sectionMapList){
					Response sectionResponse = cassandraOperation.getRecordById(pageSectionDbInfo.getKeySpace(),
							pageSectionDbInfo.getTableName(), (String)sect.get(JsonKey.ID));

					List<Map<String, Object>> sectionResult = (List<Map<String, Object>>) sectionResponse.getResult()
							.get(JsonKey.RESPONSE);
					if(null != sectionResult && sectionResult.size()>0){
						sectionResult.get(0).put(JsonKey.POSITION, sect.get(JsonKey.POSITION));
						removeUnwantedData(sectionResult.get(0), "getPageData");
						subSectionList.add(sectionResult.get(0));
					}
				}
				map.put(JsonKey.SUB_SECTIONS, subSectionList);
				sections.add(map);
			}
			
		} catch (JsonParseException e) {
			logger.error(e.getMessage(), e);
		} catch (JsonMappingException e) {
			logger.error(e.getMessage(), e);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		return sections;
	}
	
}
