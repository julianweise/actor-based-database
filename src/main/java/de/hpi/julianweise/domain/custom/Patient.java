package de.hpi.julianweise.domain.custom;

import de.hpi.julianweise.domain.ADBEntityType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@AllArgsConstructor
public class Patient implements ADBEntityType {

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
    public Comparable<String> getPrimaryKey() {
        return this.psid;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.getPrimaryKey(), this.berichtsjahr);
    }
}
