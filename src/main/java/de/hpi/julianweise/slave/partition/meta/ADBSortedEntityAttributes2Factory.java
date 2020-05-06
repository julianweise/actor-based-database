package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.domain.ADBEntity;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import lombok.AllArgsConstructor;

import java.lang.reflect.Field;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ADBSortedEntityAttributes2Factory {

    @AllArgsConstructor
    private static class AttributeIndexComparator implements IntComparator {

        private final List<ADBEntity> data;
        private final Function<ADBEntity, Comparable<Object>> attributeGetter;

        @Override
        public int compare(int a, int b) {
            return attributeGetter.apply(data.get(a)).compareTo(attributeGetter.apply(data.get(b)));
        }
    }

    public static Object2ObjectMap<String, ADBSortedEntityAttributes2> of(List<ADBEntity> data) {
        assert data.size() > 0;
        assert data.stream().map(ADBEntity::getClass).collect(Collectors.toSet()).size() == 1;
        Field[] attributes = data.get(0).getClass().getDeclaredFields();
        Object2ObjectMap<String, ADBSortedEntityAttributes2> results = new Object2ObjectOpenHashMap<>(attributes.length);

        for (Field field : attributes) {
            results.put(field.getName(), ADBSortedEntityAttributes2Factory.of(field.getName(), data));
        }
        return results;
    }

    // Assuming all entities provided are of the sane type
    public static ADBSortedEntityAttributes2 of(String fieldName, List<ADBEntity> data) {
        assert data.stream().map(ADBEntity::getClass).collect(Collectors.toSet()).size() == 1;

        if (data.size() < 1) {
            return new ADBSortedEntityAttributes2(fieldName, new int[0]);
        }
        int[] sortedIndices = getSortedIndices(data, ADBEntity.getGetterForField(fieldName, data.get(0).getClass()));
        return new ADBSortedEntityAttributes2(fieldName, sortedIndices);
    }

    private static int[] getSortedIndices(final List<ADBEntity> data, Function<ADBEntity, Comparable<Object>> getter) {
        int[] sortedIndices = new int[data.size()];
        for (int i = 0; i < sortedIndices.length; i++) sortedIndices[i] = i;
        IntArrays.parallelQuickSort(sortedIndices, new AttributeIndexComparator(data, getter));
        return sortedIndices;
    }
}
