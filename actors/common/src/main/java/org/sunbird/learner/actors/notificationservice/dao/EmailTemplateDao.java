package org.sunbird.learner.actors.notificationservice.dao;

import org.sunbird.common.models.response.Response;

public interface EmailTemplateDao {

  /**
   * Get email template information for given name.
   *
   * @param templateName Email template name
   * @return Response containing email template information
   */
  Response read(String templateName);
}
