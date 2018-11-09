package org.sunbird.learner.actors.bulkupload.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StorageDetails {

  private String storageType;
  private String container;
  private String objectId;

  public StorageDetails(String storageType, String container, String objectId) {
    super();
    this.storageType = storageType;
    this.container = container;
    this.objectId = objectId;
  }

  public StorageDetails() {}

  public String getStorageType() {
    return storageType;
  }

  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  public String getContainer() {
    return container;
  }

  public void setContainer(String container) {
    this.container = container;
  }

  public String getObjectId() {
    return objectId;
  }

  public void setObjectId(String objectId) {
    this.objectId = objectId;
  }

  public String toJsonString() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }
}
