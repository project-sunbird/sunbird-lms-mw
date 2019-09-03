package org.sunbird.bean;

import java.util.Arrays;
import java.util.List;

public class Migration {

    private String fileSize;
    private List<String>headers;
    private byte[] fileData;
    private List<String>mandatoryFields;
    private List<String>supportedFields;

    private Migration(MigrationBuilder migrationBuilder) {
        this.fileSize = migrationBuilder.fileSize;
        this.headers = migrationBuilder.headers;
        this.fileData = migrationBuilder.fileData;
        this.mandatoryFields=migrationBuilder.mandatoryFields;
        this.supportedFields=migrationBuilder.supportedFields;

    }
    public String getFileSize() {
        return fileSize;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public byte[] getFileData() {
        return fileData;
    }

    public List<String> getMandatoryFields() {
        return mandatoryFields;
    }

    public List<String> getSupportedFields() {
        return supportedFields;
    }


    @Override
    public String toString() {
        return "Migration{" +
                "fileSize='" + fileSize + '\'' +
                ", headers=" + headers +
                ", fileData=" + Arrays.toString(fileData) +
                ", mandatoryFields=" + mandatoryFields +
                ", supportedFields=" + supportedFields +
                '}';
    }

    public static class MigrationBuilder{

        private String fileSize;
        private List<String> headers;
        private byte[] fileData;
        private List<String>mandatoryFields;
        private List<String>supportedFields;
        public MigrationBuilder() {
        }

        public MigrationBuilder setFileSize(String fileSize) {
            this.fileSize = fileSize;
            return this;
        }

        public MigrationBuilder setHeaders(List<String> headers) {
            this.headers = headers;
            return this;
        }

        public MigrationBuilder setFileData(byte[] fileData) {
            this.fileData = fileData;
            return this;
        }
        public MigrationBuilder setMandatoryFields(List<String> mandatoryFields) {
            this.mandatoryFields = mandatoryFields;
            return this;
        }

        public MigrationBuilder setSupportedFields(List<String> supportedFields) {
            this.supportedFields = supportedFields;
            return this;
        }

        public Migration build(){
            Migration migration=new Migration(this);
            return migration;
        }
    }
}
