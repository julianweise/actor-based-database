package de.hpi.julianweise.shard.query_operation.join;

import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ADBSortedEntityAttributes implements Iterable<Comparable<?>> {

    private final int[] indices;
    private final List<ADBEntityType> data;
    private final Function<ADBEntityType, Comparable<Object>> fieldGetter;

    public static class CustomIterator implements Iterator<Comparable<?>> {

        private final ADBSortedEntityAttributes attributes;
        private int index = 0;


        public CustomIterator(ADBSortedEntityAttributes attributes) {
            this.attributes = attributes;
        }

        @Override
        public boolean hasNext() {
            return this.index < this.attributes.size();
        }
        @Override
        public Comparable<?> next() {
            return this.attributes.get(this.index++);
        }

    }

    public static Map<String, ADBSortedEntityAttributes> of(ADBJoinQuery joinQuery, List<ADBEntityType> data) {
        Map<String, ADBSortedEntityAttributes> handledAttributeNames = new HashMap<>();
        for(String field : joinQuery.getAllFields()) {
            handledAttributeNames.putIfAbsent(field, ADBSortedEntityAttributes.of(field, data));
        }
        return handledAttributeNames;
    }

    @SneakyThrows
    public static ADBSortedEntityAttributes of(String fieldName, List<ADBEntityType> data) {
        if (data.size() > 0) {
            return new ADBSortedEntityAttributes(data.get(0).getGetterForField(fieldName), data);
        }
        System.out.println("[ERROR] Creating SortedEntityAttributes for an empty list of data!");
        return new ADBSortedEntityAttributes(ADBEntityType.getGetterForField(fieldName,
                ADBEntityFactoryProvider.getInstance().getTargetClass()), data);
    }

    public ADBSortedEntityAttributes(Function<ADBEntityType, Comparable<Object>> fieldGetter,
                                     List<ADBEntityType> data)  {
        this.data = data;
        this.fieldGetter = fieldGetter;
        this.indices = this.getIndicesSortedByAttributeValue();
    }

    @SneakyThrows
    private int[] getIndicesSortedByAttributeValue() {
        List<ADBPair<Integer, Comparable<Object>>> fieldJoinAttributes = new ArrayList<>(data.size());
        for (int i = 0; i < data.size(); i++) {
            fieldJoinAttributes.add(new ADBPair<>(i, this.fieldGetter.apply(data.get(i))));
        }
        fieldJoinAttributes.sort(Comparator.comparing(ADBPair::getValue));
        int[] sortedIndices = new int[data.size()];
        for (int i = 0; i < fieldJoinAttributes.size(); i++) {
            sortedIndices[i] = fieldJoinAttributes.get(i).getKey();
        }
        return sortedIndices;
    }

    @Override
    public Iterator<Comparable<?>> iterator() {
        return new CustomIterator(this);
    }

    public int size() {
        return this.indices.length;
    }
    
    @SneakyThrows
    public Comparable<?> get(int index) {
        return fieldGetter.apply(this.data.get(this.indices[index]));
    }

    public Comparable<Object> getUsingOriginalIndex(int originalIndex) {
        return fieldGetter.apply(this.data.get(originalIndex));
    }

    public ADBPair<Comparable<?>, Integer> getWithOriginalIndex(int originalIndex) {
        return new ADBPair<>(this.getUsingOriginalIndex(originalIndex), originalIndex);
    }

    public List<ADBPair<Comparable<?>, Integer>> getAllWithOriginalIndex() {
        ArrayList<ADBPair<Comparable<?>, Integer>> columnWithIndex = new ArrayList<>(this.indices.length);
        for (int index : this.indices) {
            columnWithIndex.add(this.getWithOriginalIndex(index));
        }
        return columnWithIndex;
    }
}
