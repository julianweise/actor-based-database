package de.hpi.julianweise.csv;

import com.univocity.parsers.common.record.Record;
import de.hpi.julianweise.domain.custom.patient.PatientDeserializer;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBEntityFactory;

public class TestEntityFactory implements ADBEntityFactory {

    @Override public Class<? extends ADBEntity> getTargetClass() {
        return TestEntity.class;
    }

    @Override
    public ADBEntity build(Record row) {
        return TestEntity.builder()
                         .aInteger(row.getInt(0))
                         .bString(row.getString(1))
                         .cFloat(row.getFloat(2))
                         .build();
    }

    @Override public PatientDeserializer buildDeserializer() {
        return null;
    }
}
