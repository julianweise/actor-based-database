package de.hpi.julianweise.domain.custom.patient;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBEntityFactory;
import org.apache.commons.csv.CSVRecord;

import static java.lang.Integer.parseInt;

@SuppressWarnings("unused")
public class PatientFactory implements ADBEntityFactory {

    @Override
    public Class<? extends ADBEntity> getTargetClass() {
        return Patient.class;
    }

    @Override
    public ADBEntity build(CSVRecord record) {
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