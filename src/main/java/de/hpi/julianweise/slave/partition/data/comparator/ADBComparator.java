package de.hpi.julianweise.slave.partition.data.comparator;

import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;
import org.reflections.Reflections;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ADBComparator {

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static void buildComparatorMapping() {
        if (ADBComparator.comparatorConstructors.size() > 0) {
            return;
        }
        Reflections reflections = new Reflections("de.hpi.julianweise");
        Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(ADBTypeComparator.class);

        for (Class<?> comparator : annotated) {
            if (!ADBComparator.class.isAssignableFrom(comparator)) {
                continue;
            }
            if (Modifier.isAbstract(comparator.getModifiers()) || comparator.isInterface()) {
                continue;
            }
            ADBTypeComparator typeComparator = comparator.getAnnotation(ADBTypeComparator.class);
            comparatorConstructors.putIfAbsent(typeComparator.leftSideType(), new HashMap<>());
            comparatorConstructors.get(typeComparator.leftSideType()).put(typeComparator.rightSideType(),
                    (Constructor<ADBComparator>) comparator.getConstructor(Field.class, Field.class));
        }
    }

    @SneakyThrows
    private static ADBComparator create(Field leftSideField, Field rightSideField) {
        if (!comparatorConstructors.containsKey(leftSideField.getType())) {
            throw new RuntimeException(String.format("A2DB does not support %s", leftSideField.getType()));
        }
        if (!comparatorConstructors.get(leftSideField.getType()).containsKey(rightSideField.getType())) {
            throw new RuntimeException(String.format("A2DB does not support %s to %s comparisons",
                    leftSideField.getType(), rightSideField.getType()));
        }
        return ADBComparator.comparatorConstructors.get(leftSideField.getType()).get(rightSideField.getType())
                                                   .newInstance(leftSideField, rightSideField);
    }

    public static ADBComparator getFor(Field leftSideField, Field rightSideField) {
        Map<Field, ADBComparator> leftMap = ADBComparator.comparators.get(leftSideField);
        if (leftMap == null) {
            leftMap = new HashMap<>();
            ADBComparator comparator = ADBComparator.create(leftSideField, rightSideField);
            leftMap.put(rightSideField, comparator);
            ADBComparator.comparators.put(leftSideField, leftMap);
            return comparator;
        }
        ADBComparator comparator = leftMap.get(rightSideField);
        if (comparator != null) {
            return comparator;
        }
        comparator = ADBComparator.create(leftSideField, rightSideField);
        leftMap.put(rightSideField, comparator);
        return comparator;
    }

    private static final Map<Class<?>, Map<Class<?>, Constructor<ADBComparator>>> comparatorConstructors = new HashMap<>();
    private static final Map<Field, Map<Field, ADBComparator>> comparators = new ConcurrentHashMap<>();

    protected final Field leftSideField;
    protected final Field rightSideField;

    public ADBComparator(Field leftSideField, Field rightSideField) {
        this.leftSideField = leftSideField;
        this.rightSideField = rightSideField;
    }

    public abstract int compare(Object a, Object b);
}
