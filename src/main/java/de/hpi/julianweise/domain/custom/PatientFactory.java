package de.hpi.julianweise.domain.custom;

import de.hpi.julianweise.domain.ADBEntityFactory;
import de.hpi.julianweise.domain.ADBEntityType;
import org.apache.commons.csv.CSVRecord;

import static java.lang.Integer.parseInt;

public class PatientFactory implements ADBEntityFactory {

    @Override
    public Class<? extends ADBEntityType> getTargetClass() {
        return Patient.class;
    }

    @Override
    public ADBEntityType build(CSVRecord record) {
        return Patient.builder()
                      .ausgleichsjahr(parseInt(record.get(1), 10))
                      .berichtsjahr(parseInt(record.get(2), 10))
                      .psid2(parseInt(record.get(3), 10))
                      .psid(record.get(4))
                      .kvNrKennzeichen(parseInt(record.get(5), 10) == 1)
                      .geburtsjahr(parseInt(record.get(6), 10))
                      .geschlecht(parseInt(record.get(7)) == 2 ? 'm' : 'w')
                      .versichertenTage(parseInt(record.get(8), 10))
                      .verstorben(parseInt(record.get(9), 10) == 1)
                      .versichertentageKrankenGeld(parseInt(record.get(10), 10))
                      .build();
    }

    @Override
    public PatientDeserializer buildDeserializer() {
        return new PatientDeserializer();
    }
}
