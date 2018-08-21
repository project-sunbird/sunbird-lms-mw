package org.sunbird.learner.actors.notificationservice.dao;

import org.sunbird.common.models.response.Response;

public interface EmailTemplateDao {

  /**
   * Get email template information.
   *
   * @param templateName email template identifier
   * @return email template body
   */
  Response read(String templateName);
}
