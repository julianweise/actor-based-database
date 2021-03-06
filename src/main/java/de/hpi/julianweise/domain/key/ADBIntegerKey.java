package de.hpi.julianweise.domain.key;

import de.hpi.julianweise.slave.partition.data.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

@AllArgsConstructor
@NoArgsConstructor
public class ADBIntegerKey implements ADBKey {

    @Getter
    private int value;

    @Override
    public int compareTo(@NotNull ADBKey o) {
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
