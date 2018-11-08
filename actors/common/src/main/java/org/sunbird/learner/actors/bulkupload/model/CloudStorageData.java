package org.sunbird.learner.actors.bulkupload.model;

public class CloudStorageData {

  private String storageType;
  private String container;
  private String objectId;

  public CloudStorageData(String storageType, String container, String objectId) {
    super();
    this.storageType = storageType;
    this.container = container;
    this.objectId = objectId;
  }

  public CloudStorageData() {}

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
}
