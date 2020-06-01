package de.hpi.julianweise.domain.custom.patient;

import de.hpi.julianweise.domain.key.ADBStringKey;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@SuppressWarnings({"SpellCheckingInspection", "unused"})
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Patient extends ADBEntity {

    public int ausgleichsjahr;
    public int berichtsjahr;
    public int psid2;
    public String psid;
    public boolean kvNrKennzeichen;
    public int geburtsjahr;
    public char geschlecht;
    public int versichertenTage;
    public boolean verstorben;
    public int versichertentageKrankenGeld;

    @Override
    public ADBKey getPrimaryKey() {
        return new ADBStringKey(this.psid);
    }

    @Override public int getSize() {
        return 6 * Integer.BYTES + Character.BYTES + 2 + this.calculateStringMemoryFootprint(this.psid.length());
    }

    @Override
    public String toString() {
        return String.format("Patient %s aus %s [ %s | * %d | verstorben: %s", this.psid, this.berichtsjahr,
                this.getGeschlechtVisualized(), this.geburtsjahr, this.verstorben);
    }

    private String getGeschlechtVisualized() {
        return this.geschlecht == 'm' ? "♂" : "♀";
    }
}
