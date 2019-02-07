package org.sunbird.badge.model;

import java.io.Serializable;

public class ContentBadgeAssociation implements Serializable {

  private static final long serialVersionUID = 1L;

  private String id;
  private String contentId;
  private String badgeId;
  private boolean status;
  private String createdOn;
  private String updatedOn;
  private String createdBy;
  private String updatedBy;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getContentId() {
    return contentId;
  }

  public void setContentId(String contentId) {
    this.contentId = contentId;
  }

  public String getBadgeId() {
    return badgeId;
  }

  public void setBadgeId(String badgeId) {
    this.badgeId = badgeId;
  }

  public boolean isStatus() {
    return status;
  }

  public void setStatus(boolean status) {
    this.status = status;
  }

  public String getCreatedOn() {
    return createdOn;
  }

  public void setCreatedOn(String createdOn) {
    this.createdOn = createdOn;
  }

  public String getUpdatedOn() {
    return updatedOn;
  }

  public void setUpdatedOn(String updatedOn) {
    this.updatedOn = updatedOn;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public String getUpdatedBy() {
    return updatedBy;
  }

  public void setUpdatedBy(String updatedBy) {
    this.updatedBy = updatedBy;
  }
}
