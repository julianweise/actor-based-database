package de.hpi.julianweise.domain.custom;

import de.hpi.julianweise.domain.ADBEntityType;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Builder
@AllArgsConstructor
public class Patient extends ADBEntityType {

    public final int ausgleichsjahr;
    public final int berichtsjahr;
    public final int psid2;
    public final String psid;
    public final boolean kvNrKennzeichen;
    public final int geburtsjahr;
    public final char geschlecht;
    public final int versichertenTage;
    public final boolean verstorben;
    public final int versichertentageKrankenGeld;

    @Override
    public Comparable<String> getPrimaryKey() {
        return this.psid;
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
