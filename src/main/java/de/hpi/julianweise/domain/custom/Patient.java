package de.hpi.julianweise.domain.custom;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.domain.key.ADBStringKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@SuppressWarnings({"SpellCheckingInspection", "unused"})
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Patient extends ADBEntity {

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

    @Override public int getSize() {
        return 6 * Integer.BYTES + Character.BYTES + 2 + this.calculateStringMemoryFootprint(this.psid.length());
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
