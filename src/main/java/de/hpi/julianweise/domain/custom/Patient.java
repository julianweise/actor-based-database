package de.hpi.julianweise.domain.custom;

import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.domain.key.ADBStringKey;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Patient extends ADBEntityType {

    private final int ausgleichsjahr;
    private final int berichtsjahr;
    private final int psid2;
    private final String psid;
    private final boolean kvNrKennzeichen;
    private final int geburtsjahr;
    private final char geschlecht;
    private final int versichertenTage;
    private final boolean verstorben;
    private final int versichertentageKrankenGeld;

    @Override
    public ADBKey getPrimaryKey() {
        return new ADBStringKey(this.psid);
    }

    @Override
    public String toString() {
        return String.format("Patient %s aus %s [ %s | * %d | verstorben: %s", this.getPrimaryKey(), this.berichtsjahr,
                this.getGeschlechtVisualized(), this.geburtsjahr, this.verstorben);
    }

    private String getGeschlechtVisualized() {
        return this.geschlecht == 'm' ? "♂" : "♀";
    }
}
