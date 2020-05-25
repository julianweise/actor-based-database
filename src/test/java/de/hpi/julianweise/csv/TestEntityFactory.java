package de.hpi.julianweise.csv;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.domain.ADBEntityFactory;
import de.hpi.julianweise.domain.custom.Patient.PatientDeserializer;
import org.apache.commons.csv.CSVRecord;

import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;

public class TestEntityFactory implements ADBEntityFactory {

    @Override public Class<? extends ADBEntity> getTargetClass() {
        return TestEntity.class;
    }

    @Override
    public ADBEntity build(CSVRecord row) {
        return TestEntity.builder()
                         .aInteger(parseInt(row.get(0), 10))
                         .bString(row.get(1))
                         .cFloat(parseFloat(row.get(2)))
                         .build();
    }

    @Override public PatientDeserializer buildDeserializer() {
        return null;
    }
}
