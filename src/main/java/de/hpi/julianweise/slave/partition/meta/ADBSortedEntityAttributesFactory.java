package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinPredicateCostModel;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import org.agrona.collections.Object2ObjectHashMap;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ADBSortedEntityAttributesFactory {

    @AllArgsConstructor
    private static class AttributeIndexComparator implements IntComparator {

        private final ObjectList<ADBEntity> data;
        private final ADBComparator comparator;

        @Override
        public int compare(int a, int b) {
            return comparator.compare(this.data.get(a), this.data.get(b));
        }
    }

    public static Object2ObjectMap<String, ADBSortedEntityAttributes> of(ObjectList<ADBEntity> data) {
        assert data.size() > 0;
        assert data.stream().map(ADBEntity::getClass).collect(Collectors.toSet()).size() == 1;
        Field[] attributes = data.get(0).getClass().getDeclaredFields();
        Object2ObjectMap<String, ADBSortedEntityAttributes> results = new Object2ObjectOpenHashMap<>(attributes.length);

        for (Field field : attributes) {
            results.put(field.getName(), ADBSortedEntityAttributesFactory.of(field, data));
        }
        return results;
    }

    // Assuming all entities provided are of the sane type
    public static ADBSortedEntityAttributes of(Field field, ObjectList<ADBEntity> data) {
        assert data.stream().map(ADBEntity::getClass).collect(Collectors.toSet()).size() == 1;

        if (data.size() < 1) {
            return new ADBSortedEntityAttributes(field, new int[0]);
        }
        int[] sortedIndices = getSortedIndices(data, field);
        return new ADBSortedEntityAttributes(field, sortedIndices);
    }

    private static int[] getSortedIndices(final ObjectList<ADBEntity> data, Field field) {
        int[] sortedIndices = new int[data.size()];
        for (int i = 0; i < sortedIndices.length; i++) {
            sortedIndices[i] = i;
        }
        IntArrays.parallelQuickSort(sortedIndices, new AttributeIndexComparator(data, ADBComparator.getFor(field, field)));
        return sortedIndices;
    }

    public static ObjectList<Map<String, ADBEntityEntry>> resortByIndex(
            Map<String, ObjectList<ADBEntityEntry>> columnAttributes,
            ObjectList<ADBJoinPredicateCostModel> relevantCostModels) {
        int numberOfRows = columnAttributes.values().stream().mapToInt(ObjectList::size).max().orElse(0);
        ObjectList<Map<String, ADBEntityEntry>> resultSet = new ObjectArrayList<>(numberOfRows);
        for(int i=0; i < numberOfRows; i++) resultSet.add(new Object2ObjectHashMap<>());
        Set<String> relevantFields = relevantCostModels
                .stream()
                .flatMap(model -> Stream.of(model.getPredicate().getLeftHandSideAttribute(), model.getPredicate().getRightHandSideAttribute()))
                .collect(Collectors.toSet());
        for (String field : relevantFields) {
            for (ADBEntityEntry row : columnAttributes.get(field)) {
                resultSet.get(ADBInternalIDHelper.getEntityId(row.getId())).put(field, row);
            }
        }
        return resultSet;
    }
}
