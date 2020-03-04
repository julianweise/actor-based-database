package de.hpi.julianweise.domain.key;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class ADBFloatKey implements ADBKey {

    @Getter
    private float value;

    @Override
    public int compareTo(ADBKey o) throws ADBKeyComparisonException {
        if (!(o instanceof ADBFloatKey)) {
            throw new ADBKeyComparisonException(this.getClass(), o.getClass());
        }
        return (int) (this.value - ((ADBFloatKey) o).value);
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof ADBFloatKey)) {
            return false;
        }
        return value == ((ADBFloatKey) o).value;
    }

    @Override
    public int hashCode() {
        return new Float(value).hashCode();
    }

    @Override
    public String toString() {
        return this.value + "";
    }
}
