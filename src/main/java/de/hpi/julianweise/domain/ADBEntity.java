package de.hpi.julianweise.domain;

import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBSelectionQueryTerm;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import lombok.SneakyThrows;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public abstract class ADBEntity implements CborSerializable {

    private final static Map<String, Function<ADBEntity, Comparable<Object>>> getter = new ConcurrentHashMap<>();

    protected ADBEntity() {
    }

    public abstract ADBKey getPrimaryKey();

    public abstract int getSize();

    protected int calculateStringMemoryFootprint(int numberOfChars) {
        return ((int) Math.ceil(24 + 12 + numberOfChars + 2 * Character.BYTES) / 8) * 8;
    }

    @Override
    public int hashCode() {
        return this.getPrimaryKey().hashCode() + 13;
    }

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

    public final boolean fieldMatches(String fieldName, Comparable<Object> value, ADBQueryTerm.RelationalOperator opr) {
        return ADBEntity.matches(this.getGetterForField(fieldName).apply(this), value, opr);
    }

    public static boolean matches(Comparable<Object> a, Object b, ADBQueryTerm.RelationalOperator opr) {
        switch (opr) {
            case EQUALITY:
                return a.compareTo(b) == 0;
            case INEQUALITY:
                return a.compareTo(b) != 0;
            case LESS:
                return a.compareTo(b) < 0;
            case LESS_OR_EQUAL:
                return a.compareTo(b) <= 0;
            case GREATER:
                return a.compareTo(b) > 0;
            case GREATER_OR_EQUAL:
                return a.compareTo(b) >= 0;
            default:
                return false;
        }
    }

    public Function<ADBEntity, Comparable<Object>> getGetterForField(String field) {
        return ADBEntity.getGetterForField(field, this.getClass());
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static Function<ADBEntity, Comparable<Object>> getGetterForField(String field, Class<?> targetClass) {
        String fieldKey = targetClass.getName() + field;

        if (ADBEntity.getter.containsKey(fieldKey)) {
            return ADBEntity.getter.get(fieldKey);
        }

        MethodHandles.Lookup lookup = MethodHandles.lookup();

        Class<?> returnType = targetClass.getDeclaredField(field).getType();

        String getter = (returnType == boolean.class || returnType == Boolean.class ? "is" :
                "get") + field.substring(0, 1).toUpperCase() + field.substring(1);

        CallSite site = LambdaMetafactory.metafactory(lookup,
                "apply",
                MethodType.methodType(Function.class),
                MethodType.methodType(Object.class, Object.class),
                lookup.findVirtual(targetClass, getter, MethodType.methodType(returnType)),
                MethodType.methodType(returnType, targetClass));

        Function<ADBEntity, Comparable<Object>> fieldGetter =
                (Function<ADBEntity, Comparable<Object>>) site.getTarget().invokeExact();
        ADBEntity.getter.put(fieldKey, fieldGetter);
        return fieldGetter;
    }
}