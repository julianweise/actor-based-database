package de.hpi.julianweise.slave.partition.data.entry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;

import java.lang.reflect.Field;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.As.PROPERTY;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME;

@JsonTypeInfo(use = NAME, include = PROPERTY)
@JsonSubTypes({
                      @JsonSubTypes.Type(value = ADBEntityStringEntry.class, name = "ADBEntityStringEntry"),
                      @JsonSubTypes.Type(value = ADBEntityIntEntry.class, name = "ADBEntityIntEntry"),
                      @JsonSubTypes.Type(value = ADBEntityLongEntry.class, name = "ADBEntityLongEntry"),
                      @JsonSubTypes.Type(value = ADBEntityFloatEntry.class, name = "ADBEntityFloatEntry"),
                      @JsonSubTypes.Type(value = ADBEntityDoubleEntry.class, name = "ADBEntityDoubleEntry"),
                      @JsonSubTypes.Type(value = ADBEntityBooleanEntry.class, name = "ADBEntityBooleanEntry"),
                      @JsonSubTypes.Type(value = ADBEntityByteEntry.class, name = "ADBEntityByteEntry"),
                      @JsonSubTypes.Type(value = ADBEntityCharEntry.class, name = "ADBEntityCharEntry"),
                      @JsonSubTypes.Type(value = ADBEntityShortEntry.class, name = "ADBEntityShortEntry"),
              })
public interface ADBEntityEntry {

    static boolean matches(ADBEntityEntry a, ADBEntityEntry b, ADBQueryTerm.RelationalOperator operator,
                           ADBComparator comparator) {
        switch (operator) {
            case EQUALITY: return comparator.compare(a, b) == 0;
            case LESS: return comparator.compare(a, b) < 0;
            case LESS_OR_EQUAL: return comparator.compare(a, b) <= 0;
            case GREATER: return comparator.compare(a, b) > 0;
            case GREATER_OR_EQUAL: return comparator.compare(a, b) >= 0;
            case INEQUALITY: return comparator.compare(a, b) != 0;
            default: return false;
        }
    }

    int getId();

    @JsonIgnore
    Field getValueField();

    @Override
    int hashCode();

    boolean isNull();
}
