package de.hpi.julianweise.domain;

import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.commons.csv.CSVRecord;

public interface ADBEntityFactory {
    Class<? extends ADBEntity> getTargetClass();

    ADBEntity build(CSVRecord row);

    JsonDeserializer<? extends ADBEntity> buildDeserializer();
}
