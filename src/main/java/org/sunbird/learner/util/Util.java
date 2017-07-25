package org.sunbird.learner.util;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.models.util.ProjectUtil.OrgStatus;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.CassandraConnectionManager;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Utility class for actors
 * @author arvind .
 */
public class Util {

    public static Map<String, DbInfo> dbInfoMap = new HashMap<String, DbInfo>();
    public static final int RECOMENDED_LIST_SIZE = 10;
    
    private static final String KEY_SPACE_NAME = "sunbird";
    private static Properties prop = new Properties();
    private static Map<String, String> headers = new HashMap<String, String>();
    private static Map<Integer , List<Integer>> orgStatusTransition = new HashMap<Integer , List<Integer>>();
    
    
    static {
        loadPropertiesFile();
        initializeOrgStatusTransition();
        initializeDBProperty();
        // EkStep HttpClient headers init
        headers.put("content-type", "application/json");
        headers.put("accept", "application/json");
    }

    /**
     * This method will a map of organization state transaction. 
     * which will help us to move the organization status from one 
     * Valid state to another state.
     */
    private static void initializeOrgStatusTransition() {
        orgStatusTransition.put(OrgStatus.ACTIVE.getValue(), Arrays.asList(OrgStatus.ACTIVE.getValue(),OrgStatus.INACTIVE.getValue(),OrgStatus.BLOCKED.getValue(),OrgStatus.RETIRED.getValue()));
        orgStatusTransition.put(OrgStatus.INACTIVE.getValue() , Arrays.asList(OrgStatus.ACTIVE.getValue(),OrgStatus.INACTIVE.getValue()));
        orgStatusTransition.put(OrgStatus.BLOCKED.getValue() ,Arrays.asList(OrgStatus.ACTIVE.getValue(),OrgStatus.BLOCKED.getValue(),OrgStatus.RETIRED.getValue()));
        orgStatusTransition.put(OrgStatus.RETIRED.getValue() ,Arrays.asList(OrgStatus.RETIRED.getValue()));
    }
    
    /**
     * This method will initialize the cassandra data base
     * property 
     */
    private static void initializeDBProperty() {
      //setting db info (keyspace , table) into static map
      //this map will be used during cassandra data base interaction.
      //this map will have each DB name and it's corresponding keyspace and table name.
      dbInfoMap.put(JsonKey.LEARNER_COURSE_DB, getDbInfoObject(KEY_SPACE_NAME,"user_courses"));
      dbInfoMap.put(JsonKey.LEARNER_CONTENT_DB, getDbInfoObject(KEY_SPACE_NAME,"content_consumption"));
      dbInfoMap.put(JsonKey.COURSE_MANAGEMENT_DB, getDbInfoObject(KEY_SPACE_NAME,"course_management"));
      dbInfoMap.put(JsonKey.USER_DB, getDbInfoObject(KEY_SPACE_NAME,"user"));
      dbInfoMap.put(JsonKey.USER_AUTH_DB, getDbInfoObject(KEY_SPACE_NAME,"user_auth"));
      dbInfoMap.put(JsonKey.ORG_DB, getDbInfoObject(KEY_SPACE_NAME,"organisation"));
      dbInfoMap.put(JsonKey.PAGE_MGMT_DB, getDbInfoObject(KEY_SPACE_NAME,"page_management"));
      dbInfoMap.put(JsonKey.PAGE_SECTION_DB, getDbInfoObject(KEY_SPACE_NAME,"page_section"));
      dbInfoMap.put(JsonKey.SECTION_MGMT_DB, getDbInfoObject(KEY_SPACE_NAME,"page_section"));
      dbInfoMap.put(JsonKey.ASSESSMENT_EVAL_DB, getDbInfoObject(KEY_SPACE_NAME,"assessment_eval"));
      dbInfoMap.put(JsonKey.ASSESSMENT_ITEM_DB, getDbInfoObject(KEY_SPACE_NAME,"assessment_item"));
      dbInfoMap.put(JsonKey.ADDRESS_DB, getDbInfoObject(KEY_SPACE_NAME,"address"));
      dbInfoMap.put(JsonKey.EDUCATION_DB, getDbInfoObject(KEY_SPACE_NAME,"user_education"));
      dbInfoMap.put(JsonKey.JOB_PROFILE_DB, getDbInfoObject(KEY_SPACE_NAME,"user_job_profile"));
      dbInfoMap.put(JsonKey.USR_ORG_DB, getDbInfoObject(KEY_SPACE_NAME,"user_org"));
      dbInfoMap.put(JsonKey.USR_EXT_ID_DB, getDbInfoObject(KEY_SPACE_NAME,"user_external_identity"));

      dbInfoMap.put(JsonKey.ORG_MAP_DB, getDbInfoObject(KEY_SPACE_NAME,"org_mapping"));
      dbInfoMap.put(JsonKey.ORG_TYPE_DB, getDbInfoObject(KEY_SPACE_NAME,"org_type"));
      dbInfoMap.put(JsonKey.ROLE, getDbInfoObject(KEY_SPACE_NAME,"role"));
      dbInfoMap.put(JsonKey.MASTER_ACTION, getDbInfoObject(KEY_SPACE_NAME,"master_action"));
      dbInfoMap.put(JsonKey.URL_ACTION, getDbInfoObject(KEY_SPACE_NAME,"url_action"));
      dbInfoMap.put(JsonKey.ACTION_GROUP, getDbInfoObject(KEY_SPACE_NAME,"action_group"));
      dbInfoMap.put(JsonKey.USER_ACTION_ROLE, getDbInfoObject(KEY_SPACE_NAME,"user_action_role"));
      dbInfoMap.put(JsonKey.ROLE_GROUP, getDbInfoObject(KEY_SPACE_NAME,"role_group"));
      dbInfoMap.put(JsonKey.USER_ORG_DB , getDbInfoObject(KEY_SPACE_NAME , "user_org"));
    }
    
    
    /**
     * This method will take org current state and next state and check 
     * is it possible to move organization from current state to next state 
     * if possible to move then return true else false.
     * @param currentState String
     * @param nextState String
     * @return boolean
     */
    @SuppressWarnings("rawtypes")
    public static boolean checkOrgStatusTransition(Integer currentState , Integer nextState){
        List list = (List)orgStatusTransition.get(currentState);
        if(null == list){
            return false;
        }
        return list.contains(nextState);
    }
    
    /**
     * This method will check the cassandra data base connection.
     * first it will try to established the data base connection from 
     * provided environment variable , if environment variable values 
     * are not set then connection will be established from property file.
     */
    public static void checkCassandraDbConnections() {
        if (readConfigFromEnv()) {
            ProjectLogger.log("db connection is created from System env variable.");
            return ;
        }   
        String[] ipList=prop.getProperty(JsonKey.DB_IP).split(",");
        String[] portList=prop.getProperty(JsonKey.DB_PORT).split(",");
        String[] keyspaceList=prop.getProperty(JsonKey.DB_KEYSPACE).split(",");

        String userName=prop.getProperty(JsonKey.DB_USERNAME);
        String password=prop.getProperty(JsonKey.DB_PASSWORD);
        for(int i=0;i<ipList.length;i++) {
            String ip = ipList[i];
            String port = portList[i];
            String keyspace = keyspaceList[i];

            try {

                boolean result = CassandraConnectionManager.createConnection(ip, port, userName,
                        password, keyspace);
                if (result) {
                      ProjectLogger.log("CONNECTION CREATED SUCCESSFULLY FOR IP: " + ip + " : KEYSPACE :" + keyspace, LoggerEnum.INFO.name());
                    } else {
                      ProjectLogger.log("CONNECTION CREATION FAILED FOR IP: " + ip + " : KEYSPACE :" + keyspace);
                    }

            } catch (ProjectCommonException ex) {
                ProjectLogger.log(ex.getMessage(), ex);
                }

        }

    }
    
    /**
     * This method will read the configuration from System variable.
     * @return boolean
     */
    public static boolean readConfigFromEnv() {
        boolean response = false;
        String ips = System.getenv(JsonKey.SUNBIRD_CASSANDRA_IP);
        String envPort = System.getenv(JsonKey.SUNBIRD_CASSANDRA_PORT);
        
        if (ProjectUtil.isStringNullOREmpty(ips) || ProjectUtil.isStringNullOREmpty(envPort) ) {
            ProjectLogger.log("Configuration value is not coming form System variable.");
            return response;
        }
        String[] ipList = ips.split(",");
        String[] portList = envPort.split(",");
        String userName = System.getenv(JsonKey.SUNBIRD_CASSANDRA_USER_NAME);
        String password = System.getenv(JsonKey.SUNBIRD_CASSANDRA_PASSWORD);
        for (int i = 0; i < ipList.length; i++) {
            String ip = ipList[i];
            String port = portList[i];
            try {
                boolean result = CassandraConnectionManager.createConnection(ip, port, userName, password, KEY_SPACE_NAME);
                if (result) {
                    ProjectLogger.log("CONNECTION CREATED SUCCESSFULLY FOR IP: " + ip + " : KEYSPACE :" + KEY_SPACE_NAME, LoggerEnum.INFO.name());
                } else {
                    ProjectLogger.log("CONNECTION CREATION FAILED FOR IP: " + ip + " : KEYSPACE :" + KEY_SPACE_NAME, LoggerEnum.INFO.name());
                }
                response = true;
            } catch (ProjectCommonException ex) {
                ProjectLogger.log(ex.getMessage(), ex);
                throw new ProjectCommonException(ResponseCode.invaidConfiguration.getErrorCode(),
                        ResponseCode.invaidConfiguration.getErrorCode(), ResponseCode.SERVER_ERROR.hashCode());
            }
        }
        return response;
    }
    
    /**
     * This method will load the db config properties file.
     */
    private static void loadPropertiesFile() {

        InputStream input = null;

        try {
            input = Util.class.getClassLoader().getResourceAsStream("dbconfig.properties");
            // load a properties file
            prop.load(input);
        } catch (IOException ex) {
            ProjectLogger.log(ex.getMessage(), ex);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                  ProjectLogger.log(e.getMessage(), e);
                }
            }
        }

    }
    
    public static String getProperty(String key){
          return prop.getProperty(key);
       }


    private static DbInfo getDbInfoObject(String keySpace, String table) {

        DbInfo dbInfo = new DbInfo();

        dbInfo.setKeySpace(keySpace);
        dbInfo.setTableName(table);

        return dbInfo;
    }

    /**
     * class to hold cassandra db info.
     */
    public static class DbInfo {
        String keySpace;
        String tableName;
        String userName;
        String password;
        String ip;
        String port;

        /**
         * @param keySpace
         * @param tableName
         * @param userName
         * @param password
         */
        DbInfo(String keySpace, String tableName, String userName, String password, String ip, String port) {
            this.keySpace = keySpace;
            this.tableName = tableName;
            this.userName = userName;
            this.password = password;
            this.ip = ip;
            this.port = port;
        }

        /**
         * No-arg constructor
         */
        DbInfo() {
        }

        @Override
        public boolean equals(Object obj){
            if(obj instanceof DbInfo){
               DbInfo ob = (DbInfo)obj;
               if(this.ip.equals(ob.getIp()) && this.port.equals(ob.getPort()) &&
                       this.keySpace.equals(ob.getKeySpace())){
                   return true;
               }
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 1;
        }

        public String getKeySpace() {
            return keySpace;
        }

        public void setKeySpace(String keySpace) {
            this.keySpace = keySpace;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }
    }
    
    /**
     * This method will take the  map<String,Object> and list<Stirng> of keys.
     * it will remove all the keys from the source map.
     * @param map  Map<String, Object>
     * @param keys List<String> 
     */
    public static void removeAttributes(Map<String, Object> map ,List<String> keys ){

        if(null != map && null != keys) {
            for (String key : keys) {
                map.remove(key);
            }
        }
    }
    
    /**
     * This method will take searchQuery map and internally it will convert map to
     * SearchDto object.
     * @param searchQueryMap Map<String , Object>
     * @return SearchDTO
     */
    @SuppressWarnings("unchecked")
    public static SearchDTO createSearchDto(Map<String , Object> searchQueryMap){
        SearchDTO search = new SearchDTO();
        if(searchQueryMap.containsKey(JsonKey.QUERY)){
         search.setQuery((String) searchQueryMap.get(JsonKey.QUERY));
        }
        if(searchQueryMap.containsKey(JsonKey.FACETS)){
         search.setFacets((List<String>) searchQueryMap.get(JsonKey.FACETS));
        }
        if(searchQueryMap.containsKey(JsonKey.FIELDS)){
            search.setFields((List<String>) searchQueryMap.get(JsonKey.FIELDS));
        }
        if(searchQueryMap.containsKey(JsonKey.FILTERS)){
            search.getAdditionalProperties().put(JsonKey.FILTERS,searchQueryMap.get(JsonKey.FILTERS));
        }
        if(searchQueryMap.containsKey(JsonKey.EXISTS)){
            search.getAdditionalProperties().put(JsonKey.EXISTS, searchQueryMap.get(JsonKey.EXISTS));
        }
        if(searchQueryMap.containsKey(JsonKey.NOT_EXISTS)){
            search.getAdditionalProperties().put(JsonKey.NOT_EXISTS, searchQueryMap.get(JsonKey.NOT_EXISTS));
        }
        if(searchQueryMap.containsKey(JsonKey.SORT_BY)){
            search.getSortBy().putAll((Map<? extends String, ? extends String>) searchQueryMap.get(JsonKey.SORT_BY));
        }
        if(searchQueryMap.containsKey(JsonKey.OFFSET)){
            if((searchQueryMap.get(JsonKey.OFFSET)) instanceof Integer ){
                search.setOffset((int)searchQueryMap.get(JsonKey.OFFSET));
            }else{
                search.setOffset(((BigInteger) searchQueryMap.get(JsonKey.OFFSET)).intValue());
            }
        }
        if(searchQueryMap.containsKey(JsonKey.LIMIT)){
            if((searchQueryMap.get(JsonKey.LIMIT)) instanceof Integer ){
                search.setLimit((int)searchQueryMap.get(JsonKey.LIMIT));
            }else{
                search.setLimit(((BigInteger) searchQueryMap.get(JsonKey.LIMIT)).intValue());
            }
        }
        if(searchQueryMap.containsKey(JsonKey.GROUP_QUERY)){
            search.getGroupQuery().addAll((Collection<? extends Map<String, Object>>) searchQueryMap.get(JsonKey.GROUP_QUERY));
        }
        return search;
    }
    
    /**
     * This method will make a call to EKStep content search api
     * and final response will be appended with same requested map, 
     * with key "contents". Requester can read this key to collect 
     * the response.
     * @param section String, Object> 
     */
    public static void getContentData(Map<String, Object> section) {
        String response = "";
        JSONObject data;
        JSONObject jObject;
        ObjectMapper mapper = new ObjectMapper();
        try {
          String baseSearchUrl = System.getenv(JsonKey.EKSTEP_CONTENT_SEARCH_BASE_URL);
          if(ProjectUtil.isStringNullOREmpty(baseSearchUrl)){
            baseSearchUrl = PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTENT_SEARCH_BASE_URL);
          }
          headers.put(JsonKey.AUTHORIZATION, System.getenv(JsonKey.AUTHORIZATION));
          if(ProjectUtil.isStringNullOREmpty((String)headers.get(JsonKey.AUTHORIZATION))){
            headers.put(JsonKey.AUTHORIZATION, JsonKey.BEARER+PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_AUTHORIZATION));
          }
            response = HttpUtil.sendPostRequest(baseSearchUrl+PropertiesCache.getInstance().getProperty(JsonKey.EKSTEP_CONTNET_SEARCH_URL),
                    (String) section.get(JsonKey.SEARCH_QUERY), headers);
            jObject = new JSONObject(response);
            data = jObject.getJSONObject(JsonKey.RESULT);
            JSONArray contentArray = data.getJSONArray(JsonKey.CONTENT);
            section.put(JsonKey.CONTENTS, mapper.readValue(contentArray.toString(), Object[].class));
        } catch (IOException | JSONException e) {
            ProjectLogger.log(e.getMessage(), e);
        } 
    }
    
    /**
     * if Object is null then it will return true else false.
     * @param obj Object
     * @return boolean
     */
    public static boolean isNull(Object obj){
        return null == obj ? true:false;
    }
    
    /** 
     * if Object is not null then it will return true else false.
     * @param obj Object
     * @return boolean
     */
    public static boolean isNotNull(Object obj){
        return null != obj? true:false;
    }

    /**
     * This method will provide user name based on user id if user not found
     * then it will return null.
     *
     * @param userId String
     * @return String
     */
    @SuppressWarnings("unchecked")
    public static String getUserNamebyUserId(String userId) {
        CassandraOperation cassandraOperation = new CassandraOperationImpl();
        Util.DbInfo userdbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        Response result = cassandraOperation.getRecordById(userdbInfo.getKeySpace(), userdbInfo.getTableName(), userId);
        List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        if (!(list.isEmpty())) {
            return (String) (list.get(0).get(JsonKey.USERNAME));
        }
        return null;
    }
}
