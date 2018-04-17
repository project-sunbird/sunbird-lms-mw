package org.sunbird.services.service;

import org.sunbird.common.models.response.Response;
import org.sunbird.util.lmaxdisruptor.TelemetryData;

public interface TelemetryDao {
  /**
   * @description method to save TelemetryData
   * @param teleData
   * @return Response
   */
  Response save(TelemetryData teleData);
}
