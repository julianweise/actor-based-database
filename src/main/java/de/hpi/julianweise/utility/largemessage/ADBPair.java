package de.hpi.julianweise.utility.largemessage;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class ADBPair<A, B> {
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({
                          @JsonSubTypes.Type(value = String.class, name = "String"),
                          @JsonSubTypes.Type(value = int.class, name = "Integer"),
                          @JsonSubTypes.Type(value = float.class, name = "Float"),
                          @JsonSubTypes.Type(value = double.class, name = "Double"),
                          @JsonSubTypes.Type(value = char.class, name = "Character"),
                          @JsonSubTypes.Type(value = boolean.class, name = "Boolean"),
                          @JsonSubTypes.Type(value = ADBEntity.class, name = "ADBEntity"),
                  })
    private A key;
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({
                          @JsonSubTypes.Type(value = String.class, name = "String"),
                          @JsonSubTypes.Type(value = int.class, name = "Integer"),
                          @JsonSubTypes.Type(value = float.class, name = "Float"),
                          @JsonSubTypes.Type(value = double.class, name = "Double"),
                          @JsonSubTypes.Type(value = char.class, name = "Character"),
                          @JsonSubTypes.Type(value = ADBEntity.class, name = "ADBEntity"),
                  })
    private B value;

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
        if (!(o instanceof ADBPair)) {
            return false;
        }
        return this.key.equals(((ADBPair<A,B>) o).key) && this.value.equals(((ADBPair<A,B>) o).value);
    }

    @Override
    public String toString() {
        return this.key.toString() + " : " + this.value.toString();
    }
}
