package main.de.hpi.julianweise.csv;

import de.hpi.julianweise.domain.ADBEntityFactory;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.custom.PatientDeserializer;
import org.apache.commons.csv.CSVRecord;

import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;

public class TestEntityFactory implements ADBEntityFactory {

    @Override
    public ADBEntityType build(CSVRecord row) {
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
