package de.hpi.julianweise.domain;

import com.sun.xml.internal.ws.util.StringUtils;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

public abstract class ADBEntityType implements CborSerializable {

    protected ADBEntityType() {
        for (Field field : this.getClass().getDeclaredFields()) {
            field.setAccessible(true);
        }
    }

    public abstract Comparable<?> getPrimaryKey();

    public final boolean matches(ADBQuery query) {
        return query.getTerms().stream()
                    .allMatch(this::matches);
    }

    public final boolean matches(ADBQuery.ABDQueryTerm term) {
        return hasField(term.getFieldName()) && fieldMatches(term.getFieldName(),
                (Comparable<Object>) term.getValue(), term.getOperator());
    }

    public final boolean hasField(String name) {
        return Arrays.stream(this.getClass().getDeclaredFields())
                     .map(Field::getName)
                     .anyMatch(s -> s.equals(name));
    }

    @SneakyThrows
    protected final boolean fieldMatches(String fieldName, Comparable<Object> value,
                                         ADBQuery.RelationalOperator operator) {
        Object fieldValue = this.getGetterForFieldName(fieldName).invoke(this);
        switch (operator) {
            case EQUALITY:
                return value.compareTo(fieldValue) == 0;
            case INEQUALITY:
                return value.compareTo(fieldValue) != 0;
            case LESS:
                return value.compareTo(fieldValue) < 0;
            case LESS_OR_EQUAL:
                return value.compareTo(fieldValue) <= 0;
            case GREATER:
                return value.compareTo(fieldValue) > 0;
            case GREATER_OR_EQUAL:
                return value.compareTo(fieldValue) >= 0;
            default:
                return false;
        }
    }

    protected final Method getGetterForFieldName(String fieldName) throws NoSuchFieldException, NoSuchMethodException {
        Field targetField = this.getClass().getDeclaredField(fieldName);
        String capitalizedFieldName = StringUtils.capitalize(targetField.getName());
        if (targetField.getType().getName().equals("boolean")) {
            return this.getClass().getMethod("is" + capitalizedFieldName);
        }
        return this.getClass().getMethod("get" + capitalizedFieldName);
    }
}