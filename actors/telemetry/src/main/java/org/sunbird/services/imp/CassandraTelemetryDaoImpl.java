package org.sunbird.services.imp;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.BadgingJsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.services.service.TelemetryDao;
import org.sunbird.util.lmaxdisruptor.TelemetryData;

public class CassandraTelemetryDaoImpl implements TelemetryDao {

    private Util.DbInfo teleDbInfo = Util.dbInfoMap.get(BadgingJsonKey.TELEMETRY_DB);
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();

    @Override
    public Response save(TelemetryData teleData) {
        return cassandraOperation.upsertRecord(teleDbInfo.getKeySpace(), teleDbInfo.getTableName(),
                teleData.asMap());
    }

}
