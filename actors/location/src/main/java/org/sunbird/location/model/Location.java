package org.sunbird.location.model;

import java.io.Serializable;

/** @author Amit Kumar */
public class Location implements Serializable {

  private static final long serialVersionUID = -7967252522327069670L;

  private String id;
  private String code;
  private String name;
  private String locationType;
  private String parentId;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getLocationType() {
    return locationType;
  }

  public void setLocationType(String locationType) {
    this.locationType = locationType;
  }

  public String getParentId() {
    return parentId;
  }

  public void setParentId(String parentId) {
    this.parentId = parentId;
  }
}
