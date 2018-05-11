package org.sunbird.services.imp;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.services.service.TelemetryDao;
import org.sunbird.util.lmaxdisruptor.TelemetryData;

public class CassandraTelemetryDaoImpl implements TelemetryDao {

  private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

  @Override
  public Response save(TelemetryData teleData) {
    return cassandraOperation.upsertRecord("sunbird", "telemetry_raw_data", teleData.asMap());
  }
}
