package de.hpi.julianweise.csv;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.domain.key.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class TestEntity extends ADBEntity {

    private int aInteger;
    private String bString;
    private float cFloat;
    private boolean dBoolean;
    private double eDouble;
    private char fChar;

    @Override
    public ADBKey getPrimaryKey() {
        return new ADBIntegerKey(aInteger);
    }

    @Override
    public int getSize() {
        return Integer.BYTES + Float.BYTES + 1 + Double.BYTES + Character.BYTES
                + this.calculateStringMemoryFootprint(this.bString.length());
    }
}
