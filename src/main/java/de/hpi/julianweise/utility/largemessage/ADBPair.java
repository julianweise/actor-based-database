package de.hpi.julianweise.utility.largemessage;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.julianweise.domain.ADBEntityType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Objects;

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
                          @JsonSubTypes.Type(value = ADBEntityType.class, name = "ADBEntityType"),
                  })
    private A key;
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({
                          @JsonSubTypes.Type(value = String.class, name = "String"),
                          @JsonSubTypes.Type(value = Integer.class, name = "Integer"),
                          @JsonSubTypes.Type(value = Float.class, name = "Float"),
                          @JsonSubTypes.Type(value = Double.class, name = "Double"),
                          @JsonSubTypes.Type(value = Character.class, name = "Character"),
                          @JsonSubTypes.Type(value = ADBEntityType.class, name = "ADBEntityType"),
                  })
    private B value;
}
