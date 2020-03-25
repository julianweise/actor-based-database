package de.hpi.julianweise.csv;

import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.domain.key.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder
@AllArgsConstructor
@Getter
public class TestEntity extends ADBEntityType {

    private final int aInteger;
    private final String bString;
    private final float cFloat;
    private final boolean dBoolean;
    private final double eDouble;
    private final char fChar;

    @Override
    public ADBKey getPrimaryKey() {
        return new ADBIntegerKey(aInteger);
    }
}
