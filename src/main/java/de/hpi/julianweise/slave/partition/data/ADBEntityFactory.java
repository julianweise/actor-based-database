package de.hpi.julianweise.slave.partition.data;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.univocity.parsers.common.record.Record;

public interface ADBEntityFactory {
    Class<? extends ADBEntity> getTargetClass();

    ADBEntity build(Record row);

    JsonDeserializer<? extends ADBEntity> buildDeserializer();
}
