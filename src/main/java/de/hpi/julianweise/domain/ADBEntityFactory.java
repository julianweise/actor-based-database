package de.hpi.julianweise.domain;

import de.hpi.julianweise.domain.custom.PatientDeserializer;
import org.apache.commons.csv.CSVRecord;

public interface ADBEntityFactory {
    Class<? extends ADBEntityType> getTargetClass();

    ADBEntityType build(CSVRecord row);

    PatientDeserializer buildDeserializer();
}
