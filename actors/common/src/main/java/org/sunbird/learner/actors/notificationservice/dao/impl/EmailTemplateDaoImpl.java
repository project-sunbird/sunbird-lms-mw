package org.sunbird.learner.actors.notificationservice.dao.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.notificationservice.dao.EmailTemplateDao;

public class EmailTemplateDaoImpl implements EmailTemplateDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  static EmailTemplateDao emailTemplateDao;
  private static final String EMAIL_TEMPLATE = "email_template";
  private static final String DEFAULT_EMAIL_TEMPLATE_NAME = "default";
  private static final String TEMPLATE = "template";

  public static EmailTemplateDao getInstance() {
    if (emailTemplateDao == null) {
      emailTemplateDao = new EmailTemplateDaoImpl();
    }
    return emailTemplateDao;
  }

  @Override
  public String getOrDefault(String templateName) {
    List<Object> idList = new ArrayList<>();
    idList.add(templateName);
    idList.add(DEFAULT_EMAIL_TEMPLATE_NAME);
    Response response =
        cassandraOperation.getRecordsByProperty(
            JsonKey.SUNBIRD, EMAIL_TEMPLATE, JsonKey.NAME, idList, null);
    List<Map<String, Object>> respMapList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    Map<String, Object> map = null;
    if (CollectionUtils.isNotEmpty(respMapList) && respMapList.size() == 1) {
      map = respMapList.get(0);
    } else {
      Optional<Map<String, Object>> userMap =
          respMapList
              .stream()
              .filter(
                  emailTemplate -> {
                    if (((String) emailTemplate.get(JsonKey.NAME)).equalsIgnoreCase(templateName)) {
                      return true;
                    }
                    return false;
                  })
              .findFirst();
      if (userMap.isPresent()) {
        map = userMap.get();
      } else {
        map = respMapList.get(0);
      }
    }
    String fileContent = (String) map.get(TEMPLATE);
    return fileContent;
  }
}
