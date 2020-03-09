package de.hpi.julianweise.domain;

import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBSelectionQueryTerm;
import de.hpi.julianweise.utility.CborSerializable;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ADBEntityType implements CborSerializable {

    private static Map<String, Field> fields = new ConcurrentHashMap<>();

    protected ADBEntityType() {}

    public abstract ADBKey getPrimaryKey();

    public final boolean matches(ADBSelectionQuery query) {
        for (ADBSelectionQueryTerm term : query.getTerms()) {
            if (!this.matches(term)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    public final boolean matches(ADBSelectionQueryTerm term) {
        return fieldMatches(term.getFieldName(), (Comparable<Object>) term.getValue(), term.getOperator());
    }

    protected final boolean fieldMatches(String fieldName, Comparable<Object> value,
                                         ADBQueryTerm.RelationalOperator operator) {
        Object fieldValue;
        try {
            fieldValue = this.getFieldForName(fieldName).get(this);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            return false;
        }
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

    public Field getFieldForName(String fieldName) throws NoSuchFieldException {
        String fieldKey = this.getClass().getName() + fieldName;
        Field targetField = ADBEntityType.fields.get(fieldKey);
        if (targetField != null) {
            return targetField;
        }
        targetField = this.getClass().getDeclaredField(fieldName);
        targetField.setAccessible(true);
        ADBEntityType.fields.put(fieldKey, targetField);
        return targetField;
    }
}