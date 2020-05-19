package de.hpi.julianweise.domain.key;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

@AllArgsConstructor
@NoArgsConstructor
public class ADBDoubleKey implements ADBKey {

    @Getter
    private double value;

    @Override
    public int compareTo(@NotNull ADBKey o) {
        if (! (o instanceof ADBDoubleKey)) {
            return -1;
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
        return Double.hashCode(this.value);
    }

    @Override
    public String toString() {
        return this.value + "";
    }
}
