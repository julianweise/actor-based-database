package de.hpi.julianweise.utility.largemessage;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.julianweise.domain.ADBEntity;
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
                          @JsonSubTypes.Type(value = Integer.class, name = "Integer"),
                          @JsonSubTypes.Type(value = Float.class, name = "Float"),
                          @JsonSubTypes.Type(value = Double.class, name = "Double"),
                          @JsonSubTypes.Type(value = Character.class, name = "Character"),
                          @JsonSubTypes.Type(value = Boolean.class, name = "Boolean"),
                          @JsonSubTypes.Type(value = ADBEntity.class, name = "ADBEntity"),
                  })
    private A key;
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({
                          @JsonSubTypes.Type(value = String.class, name = "String"),
                          @JsonSubTypes.Type(value = Integer.class, name = "Integer"),
                          @JsonSubTypes.Type(value = Float.class, name = "Float"),
                          @JsonSubTypes.Type(value = Double.class, name = "Double"),
                          @JsonSubTypes.Type(value = Character.class, name = "Character"),
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
}
