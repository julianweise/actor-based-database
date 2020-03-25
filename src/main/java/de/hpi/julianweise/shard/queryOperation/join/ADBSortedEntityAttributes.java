package de.hpi.julianweise.shard.queryOperation.join;

import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBPair;
import javafx.util.Pair;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ADBSortedEntityAttributes implements Iterable<Comparable<?>> {

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
        for (ADBQueryTerm term : joinQuery.getTerms()) {
            ADBJoinQueryTerm joinTerm = (ADBJoinQueryTerm) term;
            if (!handledAttributeNames.containsKey(joinTerm.getSourceAttributeName())) {
                String field = joinTerm.getSourceAttributeName();
                handledAttributeNames.put(field, of(field, data));
            }
            if (!handledAttributeNames.containsKey(joinTerm.getTargetAttributeName())) {
                String field = joinTerm.getTargetAttributeName();
                handledAttributeNames.put(field, of(field, data));
            }
        }
        return handledAttributeNames;
    }

    @SneakyThrows
    public static ADBSortedEntityAttributes of(String fieldName, List<ADBEntityType> data) {
        if (data.size() > 0) {
            return new ADBSortedEntityAttributes(data.get(0).getGetterForField(fieldName), data);
        }
        return new ADBSortedEntityAttributes(ADBEntityType.getGetterForField(fieldName,
                ADBEntityFactoryProvider.getInstance().getTargetClass()), data);
    }

    private final int[] indices;
    private final List<ADBEntityType> data;
    private final Function<ADBEntityType, Comparable<Object>> fieldGetter;

    public ADBSortedEntityAttributes(Function<ADBEntityType, Comparable<Object>> fieldGetter,
                                     List<ADBEntityType> data)  {
        this.data = data;
        this.fieldGetter = fieldGetter;
        this.indices = this.getFieldValues().stream().map(Pair::getKey).mapToInt(Integer::intValue).toArray();
    }

    @SneakyThrows
    private List<Pair<Integer, Comparable<Object>>> getFieldValues() {
        List<Pair<Integer, Comparable<Object>>> fieldJoinAttributes = new ArrayList<>(data.size());
        for (int i = 0; i < data.size(); i++) {
            fieldJoinAttributes.add(new Pair<>(i, fieldGetter.apply(data.get(i))));
        }
        fieldJoinAttributes.sort(Comparator.comparing(Pair::getValue));
        return fieldJoinAttributes;
    }

    @Override
    public Iterator<Comparable<?>> iterator() {
        return new CustomIterator(this);
    }

    public int size() {
        return this.data.size();
    }
    
    @SneakyThrows
    public Comparable<?> get(int index) {
        return fieldGetter.apply(this.data.get(this.indices[index]));
    }

    public int getOriginalIndex(int index) {
        return this.indices[index];
    }

    public ADBPair<Comparable<?>, Integer> getWithOriginalIndex(int index) {
        return new ADBPair<>(this.get(index), this.indices[index]);
    }

    public List<ADBPair<Comparable<?>, Integer>> getAllWithOriginalIndex() {
        return Arrays.stream(this.indices).mapToObj(this::getWithOriginalIndex).collect(Collectors.toList());
    }
}
