package main.de.hpi.julianweise.csv;

import de.hpi.julianweise.domain.ADBEntityType;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Builder
@AllArgsConstructor
public class TestEntity extends ADBEntityType {

    public final int aInteger;
    public final String bString;
    public final float cFloat;
    public final boolean dBoolean;
    public final double eDouble;
    public final char fChar;

    @Override
    public Comparable<?> getPrimaryKey() {
        return this.aInteger;
    }
}
