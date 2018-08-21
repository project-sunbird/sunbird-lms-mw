package org.sunbird.learner.actors.notificationservice.dao.impl;

import java.util.Map;
import java.util.WeakHashMap;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.notificationservice.dao.EmailTemplateDao;

public class EmailTemplateDaoImpl implements EmailTemplateDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  static EmailTemplateDao emailTemplateDao;
  private static final String EMAIL_TEMPLATE = "email_template";

  public static EmailTemplateDao getInstance() {
    if (emailTemplateDao == null) {
      emailTemplateDao = new EmailTemplateDaoImpl();
    }
    return emailTemplateDao;
  }

  @Override
  public Response read(String templateName) {
    Map<String, Object> queryMap = new WeakHashMap<>();
    queryMap.put(JsonKey.NAME, templateName);
    return cassandraOperation.getRecordById(JsonKey.SUNBIRD, EMAIL_TEMPLATE, queryMap);
  }
}
