package de.hpi.julianweise.domain.custom.patient;

import com.univocity.parsers.common.record.Record;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBEntityFactory;

@SuppressWarnings("unused")
public class PatientFactory implements ADBEntityFactory {

    @Override
    public Class<? extends ADBEntity> getTargetClass() {
        return Patient.class;
    }

    @Override
    public ADBEntity build(Record record) {
        return Patient.builder()
                      .ausgleichsjahr(record.getInt(1))
                      .berichtsjahr(record.getInt(2))
                      .psid2(record.getInt(3))
                      .psid(record.getString(4))
                      .kvNrKennzeichen(record.getInt(5) == 1)
                      .geburtsjahr(record.getInt(6))
                      .geschlecht(record.getInt(7) == 2 ? 'm' : 'w')
                      .versichertenTage(record.getInt(8))
                      .verstorben(record.getInt(9) == 1)
                      .versichertentageKrankenGeld(record.getInt(10))
                      .build();
    }

    @Override
    public PatientDeserializer buildDeserializer() {
        return new PatientDeserializer();
    }
}
