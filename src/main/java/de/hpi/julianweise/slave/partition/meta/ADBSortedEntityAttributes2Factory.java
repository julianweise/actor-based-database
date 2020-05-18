package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinTermCostModel;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import lombok.AllArgsConstructor;
import org.agrona.collections.Object2ObjectHashMap;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        for (int i = 0; i < sortedIndices.length; i++) {
            sortedIndices[i] = i;
        }
        IntArrays.parallelQuickSort(sortedIndices, new AttributeIndexComparator(data, getter));
        return sortedIndices;
    }

    public static List<Map<String, ADBComparable2IntPair>> resortByIndex(
            Map<String, List<ADBComparable2IntPair>> columnAttributes,
            List<ADBJoinTermCostModel> relevantCostModels) {
        int numberOfRows = columnAttributes.values().stream().mapToInt(List::size).max().orElse(0);
        List<Map<String, ADBComparable2IntPair>> resultSet = new ArrayList<>(numberOfRows);
        for(int i=0; i < numberOfRows; i++) resultSet.add(new Object2ObjectHashMap<>());
        Set<String> relevantFields = relevantCostModels
                .stream()
                .flatMap(model -> Stream.of(model.getTerm().getLeftHandSideAttribute(), model.getTerm().getRightHandSideAttribute()))
                .collect(Collectors.toSet());
        for (String field : relevantFields) {
            for (ADBComparable2IntPair row : columnAttributes.get(field)) {
                resultSet.get(ADBInternalIDHelper.getEntityId(row.getValue())).put(field, row);
            }
        }
        return resultSet;
    }
}
