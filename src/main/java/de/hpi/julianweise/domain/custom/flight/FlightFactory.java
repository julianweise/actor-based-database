package de.hpi.julianweise.domain.custom.flight;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.univocity.parsers.common.record.Record;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBEntityFactory;

public class FlightFactory implements ADBEntityFactory {

    @Override
    public Class<? extends ADBEntity> getTargetClass() {
        return FLight.class;
    }

    @Override
    public ADBEntity build(Record record) {
        return FLight.builder()
                     .FL_DATA(record.getInt(0))
                     .OP_UNIQUE_CARRIER(record.getString(1))
                     .ORIGIN_AIRPORT_ID(record.getShort(2))
                     .DEST_AIRPORT_ID(record.getShort(3))
                     .CRS_DEP_TIME(record.getString(4))
                     .DEP_TIME(record.getString(5))
                     .DEP_DELAY(record.getFloat(6))
                     .WHEELS_ON(record.getString(7))
                     .TAXI_IN(record.getFloat(8))
                     .CRS_ARR_TIME(record.getString(9))
                     .ARR_TIME(record.getString(10))
                     .ARR_DELAY(record.getFloat(11))
                     .CRS_ELAPSED_TIME(record.getFloat(12))
                     .ACTUAL_ELAPSED_TIME(record.getFloat(13))
                     .DISTANCE(record.getFloat(14))
                     .build();

    }

    @Override
    public JsonDeserializer<? extends ADBEntity> buildDeserializer() {
        return new FlightDeserializer();
    }
}
