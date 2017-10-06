package org.sunbird.learner.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;

public class SocialMediaType {

  private static Map<String, String> mediaTypes = new HashMap<>();
  private static CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private static Util.DbInfo mediaTypeDB = Util.dbInfoMap.get(JsonKey.MEDIA_TYPE_DB);

  public static Map<String, String> getMediaTypes() {
    if (null == mediaTypes || mediaTypes.isEmpty()) {
     updateCache();
    }
    return mediaTypes;
  }

  public void addMediaType(String id, String name) {
    Map<String, Object> queryMap = new HashMap<>();
    queryMap.put(JsonKey.ID, id);
    queryMap.put(JsonKey.NAME, name);
    Response response = cassandraOperation.insertRecord(mediaTypeDB.getKeySpace(),
        mediaTypeDB.getTableName(), queryMap);
    if (ResponseCode.success.getResponseCode() == response.getResponseCode().getResponseCode()) {
      mediaTypes.put(id, name);
    }
  }

  public static Response getMediaTypeFromDB() {
    Response response =
        cassandraOperation.getAllRecords(mediaTypeDB.getKeySpace(), mediaTypeDB.getTableName());
    return response;
  }

  @SuppressWarnings("unchecked")
  private static void updateCache() {
    Response response = getMediaTypeFromDB();
    Map<String, String> mediaMap = new HashMap<>();
    List<Map<String, Object>> list = ((List<Map<String, Object>>) response.get(JsonKey.RESPONSE));
    if (!list.isEmpty()) {
      for (Map<String, Object> data : list) {
        mediaMap.put((String) data.get(JsonKey.ID), (String) data.get(JsonKey.NAME));
      }
    }
    mediaTypes = mediaMap;
  }

  private static Boolean validateMediaURL(String type, String url) {
    String pattern = "";
    switch (type) {
      case "fb":{
        pattern = "(?:(?:http|https):\\/\\/)?(?:www.)?facebook.com\\/(?:(?:\\w)*#!\\/)?(?:pages\\/)?(?:[?\\w\\-]*\\/)?(?:profile.php\\?id=(?=\\d.*))?([\\w\\-]*)?";
        return IsMatch(url, pattern);
      }
      case "twitter":{
        pattern = "http(?:s)?:\\/\\/(?:www.)?twitter\\.com\\/([a-zA-Z0-9_]+)";
        return IsMatch(url, pattern);
      }
      case "in":{
        pattern = "((http(s?)://)*([a-zA-Z0-9\\-])*\\.|[linkedin])[linkedin/~\\-]+\\.[a-zA-Z0-9/~\\-_,&=\\?\\.;]+[^\\.,\\s<]";
        return IsMatch(url, pattern);
      }
      default: {
        pattern = "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        return IsMatch(url, pattern);
      }
    }
  }

  private static boolean IsMatch(String s, String pattern) {
    try {
      Pattern patt = Pattern.compile(pattern);
      Matcher matcher = patt.matcher(s);
      return matcher.matches();
    } catch (RuntimeException e) {
      return false;
    }
  }
  
  public static void validateSocialMedia(List<Map<String, String>> socialMediaList) {
    for(Map<String,String> socialMedia : socialMediaList){
      if(null == socialMedia || socialMedia.isEmpty()){
        throw new ProjectCommonException(ResponseCode.invalidWebPageData.getErrorCode(),
            ResponseCode.invalidWebPageData.getErrorMessage(),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
      String mediaType = socialMedia.get(JsonKey.TYPE);
      if(!SocialMediaType.getMediaTypes().containsKey(mediaType)){
        SocialMediaType.updateCache();
        if(!SocialMediaType.getMediaTypes().containsKey(mediaType)){
          throw new ProjectCommonException(ResponseCode.invalidMediaType.getErrorCode(),
              ResponseCode.invalidMediaType.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        } 
      }
      if(!SocialMediaType.validateMediaURL(mediaType, socialMedia.get(JsonKey.URL))){
        throw new ProjectCommonException(ResponseCode.invalidWebPageUrl.getErrorCode(),
            ProjectUtil.formatMessage(ResponseCode.invalidWebPageUrl.getErrorMessage(), mediaType),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }  
    } 
  }
  
}
