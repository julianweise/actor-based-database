package de.hpi.julianweise.domain.key;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
@NoArgsConstructor
public class ADBStringKey implements ADBKey {

    @NonNull
    @Getter
    private String value;

    @Override
    public int compareTo(ADBKey o) throws ADBKeyComparisonException {
        if (!(o instanceof ADBStringKey)) {
            throw new ADBKeyComparisonException(this.getClass(), o.getClass());
        }
        return value.compareTo(((ADBStringKey) o).value);
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof ADBStringKey)) {
            return false;
        }
        return value.equals(((ADBStringKey) o).value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return this.value;
    }
}
