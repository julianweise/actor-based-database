package de.hpi.julianweise.utility.largemessage;

import de.hpi.julianweise.domain.ADBEntity;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class ADBSemiMaterializedPair {

    private int key;
    private ADBEntity value;

    public ADBSemiMaterializedPair(int a, ADBEntity b) {
        this.key = a;
        this.value = b;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ADBSemiMaterializedPair)) {
            return false;
        }
        return ((ADBSemiMaterializedPair) o).key == this.key && ((ADBSemiMaterializedPair) o).value.equals(this.value);
    }
}
