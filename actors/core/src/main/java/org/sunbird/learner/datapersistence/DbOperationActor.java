package org.sunbird.learner.datapersistence;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.CassandraConnectionManager;
import org.sunbird.helper.CassandraConnectionMngrFactory;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;

import com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.UntypedAbstractActor;

public class DbOperationActor extends UntypedAbstractActor {

	private static final String REQUIRED_FIELDS = "requiredFields";
	private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
	private static CassandraConnectionManager manager = CassandraConnectionMngrFactory
			.getObject(PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_CASSANDRA_MODE));
	private static final String PAYLOAD = "payload";
	private static final String ENTITY_NAME = "entityName";
	private static final String INDEXED = "indexed";
	private static final String ES_INDEX_NAME = "sunbirdplugin";
	private static List<String> tableList = null;
	private static final String RAW_QUERY = "rawQuery";

	static {
		createtableList();
	}

	public static void createtableList() {
		try {
			tableList = manager.getTableList(JsonKey.SUNBIRD_PLUGIN);
		} catch (Exception e) {
			ProjectLogger.log("Error occured" + e.getMessage(), e);
		}
	}

	@Override
	public void onReceive(Object message) throws Throwable {
		if (null == tableList) {
			createtableList();
		}
		if (message instanceof Request) {
			ProjectLogger.log("DbOperationActor  onReceive called");
			Request actorMessage = (Request) message;
			if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.CREATE_DATA.getValue())) {
				create(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.UPDATE_DATA.getValue())) {
				update(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.DELETE_DATA.getValue())) {
				delete(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.READ_DATA.getValue())) {
				read(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.READ_ALL_DATA.getValue())) {
				readAllData(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.SEARCH_DATA.getValue())) {
				search(actorMessage);
			} else if (actorMessage.getOperation().equalsIgnoreCase(ActorOperations.GET_METRICS.getValue())) {
				getMetrics(actorMessage);
			} else {
				ProjectLogger.log("UNSUPPORTED OPERATION");
				ProjectCommonException exception = new ProjectCommonException(
						ResponseCode.invalidOperationName.getErrorCode(),
						ResponseCode.invalidOperationName.getErrorMessage(),
						ResponseCode.CLIENT_ERROR.getResponseCode());
				sender().tell(exception, self());
			}
		} else {
			ProjectLogger.log("UNSUPPORTED MESSAGE");
			ProjectCommonException exception = new ProjectCommonException(
					ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(),
					ResponseCode.CLIENT_ERROR.getResponseCode());
			sender().tell(exception, self());
		}
	}

	private void getMetrics(Request actorMessage) {
		try {
			validateTableName(actorMessage);
			Map<String, Object> rawQueryMap = (Map<String, Object>) actorMessage.getRequest().get(RAW_QUERY);
			rawQueryMap.put(JsonKey.SIZE, 0);
			ObjectMapper mapper = new ObjectMapper();
			String rawQuery = mapper.writeValueAsString(rawQueryMap);
			Response response = ElasticSearchUtil.searchMetricsData(ES_INDEX_NAME,
					(String) actorMessage.getRequest().get(ENTITY_NAME), rawQuery);
			sender().tell(response, self());
		} catch (Exception ex) {
			sender().tell(ex, self());
		}
	}

	private void search(Request reqObj) {
		try {
			Response response = new Response();
			List<String> requiredFields = null;
			if (!ProjectUtil.isStringNullOREmpty((String) reqObj.getRequest().get(ENTITY_NAME))) {
				String esType = (String) reqObj.getRequest().get(ENTITY_NAME);
				if (reqObj.getRequest().containsKey(REQUIRED_FIELDS)) {
					requiredFields = (List<String>) reqObj.getRequest().get(REQUIRED_FIELDS);
					reqObj.getRequest().remove(REQUIRED_FIELDS);
				}

				reqObj.getRequest().remove(ENTITY_NAME);
				if (null != reqObj.getRequest().get(JsonKey.FILTERS)) {
					validateRequestData((Map<String, Object>) reqObj.getRequest().get(JsonKey.FILTERS));
				}
				SearchDTO searchDto = Util.createSearchDto(reqObj.getRequest());
				Map<String, Object> result = ElasticSearchUtil.complexSearch(searchDto, ES_INDEX_NAME, esType);
				Map<String, Object> finalResult = new HashMap<>();
				if (!result.isEmpty()) {
					// filter the required fields like content or facet etc...
					if (null != requiredFields && !requiredFields.isEmpty()) {
						for (String attribute : requiredFields) {
							finalResult.put(attribute, result.get(attribute));
						}
						result = finalResult;
					}
					if (result.containsKey(JsonKey.CONTENT)) {
						List<Map<String, Object>> mapList = (List<Map<String, Object>>) result.get(JsonKey.CONTENT);
						for (Map<String, Object> map : mapList) {
							map.remove(JsonKey.IDENTIFIER);
						}
					}
					response.put(JsonKey.RESPONSE, result);
				} else {
					response.put(JsonKey.RESPONSE, new HashMap<>());
				}
			} else {
				throw new ProjectCommonException(ResponseCode.tableOrDocNameError.getErrorCode(),
						ResponseCode.tableOrDocNameError.getErrorMessage(),
						ResponseCode.CLIENT_ERROR.getResponseCode());
			}

			sender().tell(response, self());
		} catch (Exception ex) {
			sender().tell(ex, self());
		}
	}

	private void readAllData(Request reqObj) {
		try {
			Response response = null;
			validateTableName(reqObj);
			if (!ProjectUtil.isStringNullOREmpty((String) reqObj.getRequest().get(ENTITY_NAME))) {
				response = cassandraOperation.getAllRecords(JsonKey.SUNBIRD_PLUGIN,
						(String) reqObj.getRequest().get(ENTITY_NAME));
			} else {
				throw new ProjectCommonException(ResponseCode.tableOrDocNameError.getErrorCode(),
						ResponseCode.tableOrDocNameError.getErrorMessage(),
						ResponseCode.CLIENT_ERROR.getResponseCode());
			}
			sender().tell(response, self());
		} catch (Exception ex) {
			sender().tell(ex, self());
		}
	}

	private void read(Request reqObj) {
		try {
			Response response = null;
			validateTableName(reqObj);
			if (!ProjectUtil.isStringNullOREmpty((String) reqObj.getRequest().get(ENTITY_NAME))) {
				response = cassandraOperation.getRecordById(JsonKey.SUNBIRD_PLUGIN,
						(String) reqObj.getRequest().get(ENTITY_NAME), (String) reqObj.getRequest().get(JsonKey.ID));
			} else {
				throw new ProjectCommonException(ResponseCode.tableOrDocNameError.getErrorCode(),
						ResponseCode.tableOrDocNameError.getErrorMessage(),
						ResponseCode.CLIENT_ERROR.getResponseCode());
			}
			sender().tell(response, self());
		} catch (Exception ex) {
			sender().tell(ex, self());
		}
	}

	private void delete(Request reqObj) {
		try {
			validateTableName(reqObj);
			Response response = cassandraOperation.deleteRecord(JsonKey.SUNBIRD_PLUGIN,
					(String) reqObj.getRequest().get(ENTITY_NAME), (String) reqObj.getRequest().get(JsonKey.ID));
			if (((String) response.get(JsonKey.RESPONSE)).equals(JsonKey.SUCCESS)
					&& ((boolean) reqObj.getRequest().get(INDEXED))) {
				deleteDataFromElastic(ES_INDEX_NAME, (String) reqObj.getRequest().get(ENTITY_NAME),
						(String) reqObj.getRequest().get(JsonKey.ID));
			}
			sender().tell(response, self());
		} catch (Exception ex) {
			sender().tell(ex, self());
		}
	}

	private void update(Request reqObj) {
		try {
			validateTableName(reqObj);
			Map<String, Object> payload = (Map<String, Object>) reqObj.getRequest().get(PAYLOAD);
			validateRequestData(payload);
			Response response = null;
			boolean esResult = false;
			if (!ProjectUtil.isStringNullOREmpty((String) reqObj.getRequest().get(ENTITY_NAME))
					&& ((boolean) reqObj.getRequest().get(INDEXED))) {
				esResult = updateDataToElastic(ES_INDEX_NAME, (String) reqObj.getRequest().get(ENTITY_NAME),
						(String) payload.get(JsonKey.ID), payload);
				if (esResult) {
					response = cassandraOperation.updateRecord(JsonKey.SUNBIRD_PLUGIN,
							(String) reqObj.getRequest().get(ENTITY_NAME), payload);
					if (!((String) response.get(JsonKey.RESPONSE)).equals(JsonKey.SUCCESS)) {
						deleteDataFromElastic(ES_INDEX_NAME, (String) reqObj.getRequest().get(ENTITY_NAME),
								(String) payload.get(JsonKey.ID));
						throw new ProjectCommonException(ResponseCode.esUpdateFailed.getErrorCode(),
								ResponseCode.esUpdateFailed.getErrorMessage(),
								ResponseCode.SERVER_ERROR.getResponseCode());
					}
				} else {
					throw new ProjectCommonException(ResponseCode.updateFailed.getErrorCode(),
							ResponseCode.updateFailed.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
				}
			} else {
				Map<String, Object> data = ElasticSearchUtil.getDataByIdentifier(ES_INDEX_NAME,
						(String) reqObj.getRequest().get(ENTITY_NAME), (String) payload.get(JsonKey.ID));
				if (data.isEmpty() || ((boolean) reqObj.getRequest().get(INDEXED))) {
					response = cassandraOperation.updateRecord(JsonKey.SUNBIRD_PLUGIN,
							(String) reqObj.getRequest().get(ENTITY_NAME), payload);
				} else {
					throw new ProjectCommonException(ResponseCode.updateFailed.getErrorCode(),
							ResponseCode.updateFailed.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
				}
			}
			sender().tell(response, self());
		} catch (Exception ex) {
			sender().tell(ex, self());
		}
	}

	private void create(Request reqObj) {
		try {
			validateTableName(reqObj);
			Map<String, Object> payload = (Map<String, Object>) reqObj.getRequest().get(PAYLOAD);
			validateRequestData(payload);
			Response response = cassandraOperation.insertRecord(JsonKey.SUNBIRD_PLUGIN,
					(String) reqObj.getRequest().get(ENTITY_NAME), payload);
			if (((String) response.get(JsonKey.RESPONSE)).equals(JsonKey.SUCCESS)
					&& ((boolean) reqObj.getRequest().get(INDEXED))) {
				boolean esResult = false;
				if (!ProjectUtil.isStringNullOREmpty((String) reqObj.getRequest().get(ENTITY_NAME))) {
					esResult = insertDataToElastic(ES_INDEX_NAME, (String) reqObj.getRequest().get(ENTITY_NAME),
							(String) payload.get(JsonKey.ID), payload);
					if (!esResult) {
						deleteRecord(JsonKey.SUNBIRD_PLUGIN, (String) reqObj.getRequest().get(ENTITY_NAME),
								(String) payload.get(JsonKey.ID));
						throw new ProjectCommonException(ResponseCode.esUpdateFailed.getErrorCode(),
								ResponseCode.esUpdateFailed.getErrorMessage(),
								ResponseCode.SERVER_ERROR.getResponseCode());
					}
					response.put(JsonKey.DATA, payload);
				}
			}
			sender().tell(response, self());
		} catch (Exception ex) {
			sender().tell(ex, self());
		}
	}

	private void validateRequestData(Map<String, Object> payload) {
		for (Map.Entry<String, Object> entry : payload.entrySet()) {
			if (entry.getValue() instanceof BigInteger) {
				payload.put(entry.getKey(), Integer.parseInt(String.valueOf(entry.getValue())));
			}
		}
	}

	private void validateTableName(Request reqObj) {
		if (!tableList.contains((String) reqObj.getRequest().get(ENTITY_NAME))) {
			throw new ProjectCommonException(ResponseCode.tableOrDocNameError.getErrorCode(),
					ResponseCode.tableOrDocNameError.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
		}
	}

	/**
	 * Method to insert data to ES .
	 * 
	 * @param index
	 * @param type
	 * @param identifier
	 * @param data
	 * @return
	 */
	private boolean insertDataToElastic(String index, String type, String identifier, Map<String, Object> data) {
		ProjectLogger.log("making call to ES for type ,identifier ,data==" + type + " " + identifier + data);
		String response = ElasticSearchUtil.createData(index, type, identifier, data);
		ProjectLogger
				.log("Getting ES save response for type , identiofier==" + type + "  " + identifier + "  " + response);
		if (!ProjectUtil.isStringNullOREmpty(response)) {
			ProjectLogger.log("Data is saved successfully ES ." + type + "  " + identifier);
			return true;
		}
		ProjectLogger.log("unbale to save the data inside ES with identifier " + identifier, LoggerEnum.INFO.name());
		return false;
	}

	/**
	 * Method to update data to ES .
	 * 
	 * @param indexName
	 * @param typeName
	 * @param identifier
	 * @param data
	 * @return
	 */
	private boolean updateDataToElastic(String indexName, String typeName, String identifier,
			Map<String, Object> data) {
		boolean response = ElasticSearchUtil.updateData(indexName, typeName, identifier, data);
		if (response) {
			return true;
		}
		ProjectLogger.log("unable to save the data inside ES with identifier " + identifier, LoggerEnum.INFO.name());
		return false;
	}

	/**
	 * Method to update data to ES .
	 * 
	 * @param indexName
	 * @param typeName
	 * @param identifier
	 * @param data
	 * @return
	 */
	private boolean deleteDataFromElastic(String indexName, String typeName, String identifier) {
		boolean response = ElasticSearchUtil.removeData(indexName, typeName, identifier);
		if (response) {
			return true;
		}
		ProjectLogger.log("unable to delete the data from ES with identifier " + identifier, LoggerEnum.INFO.name());
		return false;
	}

	private void deleteRecord(String keyspaceName, String tableName, String identifier) {
		cassandraOperation.deleteRecord(keyspaceName, tableName, identifier);
	}
}
