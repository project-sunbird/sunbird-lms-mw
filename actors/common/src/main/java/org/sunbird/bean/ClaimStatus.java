package org.sunbird.bean;

public enum ClaimStatus {


    CLAIMED(0),
    UNCLAIMED(1),
    REJECTED(2),
    FAILED(3);
    private int value;
    ClaimStatus(int value) {
        this.value = value;
    }
    public int getValue() {
        return value;
    }
}
