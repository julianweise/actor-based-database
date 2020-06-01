package de.hpi.julianweise.csv;

import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TestEntity extends ADBEntity {

    public int aInteger;
    public String bString;
    public float cFloat;
    public boolean dBoolean;
    public double eDouble;

    @Override
    public ADBKey getPrimaryKey() {
        return new ADBIntegerKey(aInteger);
    }

    @Override
    public int getSize() {
        return Integer.BYTES + Float.BYTES + 1 + Double.BYTES
                + this.calculateStringMemoryFootprint(this.bString.length());
    }
}
