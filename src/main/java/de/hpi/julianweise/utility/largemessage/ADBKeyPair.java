package de.hpi.julianweise.utility.largemessage;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ADBKeyPair {

    private int key;
    private int value;

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ADBKeyPair)) {
            return false;
        }
        return ((ADBKeyPair) o).key == this.key && ((ADBKeyPair) o).value == this.value;
    }

    @Override
    public String toString() {
        return this.key + ", " + this.value;
    }
}
