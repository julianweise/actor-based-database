package de.hpi.julianweise.domain.key;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class ADBDoubleKey implements ADBKey {

    @Getter
    private double value;

    @Override
    public int compareTo(ADBKey o) throws ADBKeyComparisonException {
        if (!(o instanceof ADBDoubleKey)) {
            throw new ADBKeyComparisonException(this.getClass(), o.getClass());
        }
        return (int) (this.value - ((ADBDoubleKey) o).value);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ADBDoubleKey)) {
            return false;
        }
        return value == ((ADBDoubleKey) o).value;
    }


    @Override
    public int hashCode() {
        return new Double(value).hashCode();
    }

    @Override
    public String toString() {
        return this.value + "";
    }
}
