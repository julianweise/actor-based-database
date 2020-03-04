package de.hpi.julianweise.domain.key;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class ADBIntegerKey implements ADBKey {

    @Getter
    private int value;

    @Override
    public int compareTo(ADBKey o) throws ADBKeyComparisonException {
        if (!(o instanceof ADBIntegerKey)) {
            throw new ADBKeyComparisonException(this.getClass(), o.getClass());
        }
        return value - ((ADBIntegerKey) o).value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ADBIntegerKey)) {
            return false;
        }
        return value == (((ADBIntegerKey) o).value);
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public String toString() {
        return this.value + "";
    }
}
