package de.hpi.julianweise.utility.largemessage;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.julianweise.domain.ADBEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Getter
@NoArgsConstructor
public class ADBComparable2IntPair {
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({
                          @JsonSubTypes.Type(value = String.class, name = "String"),
                          @JsonSubTypes.Type(value = int.class, name = "Integer"),
                          @JsonSubTypes.Type(value = float.class, name = "Float"),
                          @JsonSubTypes.Type(value = double.class, name = "Double"),
                          @JsonSubTypes.Type(value = char.class, name = "Character"),
                          @JsonSubTypes.Type(value = ADBEntity.class, name = "ADBEntity"),
                  })
    private Comparable<Object> key;
    private int value;

    public boolean equals(Object o) {
        if (!(o instanceof ADBComparable2IntPair)) {
            return false;
        }
        return this.key.equals(((ADBComparable2IntPair) o).key) && this.value == (((ADBComparable2IntPair) o).value);
    }

    @Override
    public String toString() {
        return this.key.toString() + " : " + this.value;
    }
}
