package main.de.hpi.julianweise.csv;

import de.hpi.julianweise.domain.ADBEntityType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@AllArgsConstructor
public class TestEntity implements ADBEntityType {

    private final int aInteger;
    private final String bString;
    private final float cFloat;

    @Override
    public Comparable<?> getPrimaryKey() {
        return this.aInteger;
    }
}
