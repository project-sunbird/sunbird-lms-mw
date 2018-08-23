package org.sunbird.learner.actors.notificationservice.dao;

public interface EmailTemplateDao {

  /**
   * Get email template information for given name if not found will return the default template.
   *
   * @param templateName Email template name
   * @return String containing email template information
   */
  String getOrDefault(String templateName);
}
