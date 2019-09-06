package org.sunbird.error;

public class ErrorDetails {

    int rowId;
    private String header;
    private ErrorEnum errorEnum;

    public ErrorDetails(int rowId, String header, ErrorEnum errorEnum) {
        this.rowId = rowId;
        this.header = header;
        this.errorEnum = errorEnum;
    }

    public ErrorDetails() {
    }

    public int getRowId() {
        return rowId;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public ErrorEnum getErrorEnum() {
        return errorEnum;
    }

    public void setErrorEnum(ErrorEnum errorEnum) {
        this.errorEnum = errorEnum;
    }
}
