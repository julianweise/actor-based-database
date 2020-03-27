package de.hpi.julianweise.domain.custom;

import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.domain.key.ADBStringKey;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Patient extends ADBEntityType {

    private int ausgleichsjahr;
    private int berichtsjahr;
    private int psid2;
    private String psid;
    private boolean kvNrKennzeichen;
    private int geburtsjahr;
    private char geschlecht;
    private int versichertenTage;
    private boolean verstorben;
    private int versichertentageKrankenGeld;

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
