package org.sunbird.location.model;

import java.io.Serializable;

/** @author Amit Kumar */
public class Location implements Serializable {

  private static final long serialVersionUID = -7967252522327069670L;

  private String id;
  private String code;
  private String name;
  private String type;
  private String parentId;

  /** @return the id */
  public String getId() {
    return id;
  }
  /** @param id the id to set */
  public void setId(String id) {
    this.id = id;
  }
  /** @return the code */
  public String getCode() {
    return code;
  }
  /** @param code the code to set */
  public void setCode(String code) {
    this.code = code;
  }
  /** @return the name */
  public String getName() {
    return name;
  }
  /** @param name the name to set */
  public void setName(String name) {
    this.name = name;
  }
  /** @return the type */
  public String getType() {
    return type;
  }
  /** @param type the type to set */
  public void setType(String type) {
    this.type = type;
  }
  /** @return the parentId */
  public String getParentId() {
    return parentId;
  }
  /** @param parentId the parentId to set */
  public void setParentId(String parentId) {
    this.parentId = parentId;
  }
}
