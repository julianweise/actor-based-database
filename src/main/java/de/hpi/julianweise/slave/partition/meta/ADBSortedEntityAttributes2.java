package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

@AllArgsConstructor
@Getter
public class ADBSortedEntityAttributes2 {

    private final String field;
    private final int[] sortedIndices;

    public List<ADBComparable2IntPair> getMaterialized(List<ADBEntity> data) {
        if (data.size() < 1) {
            return Collections.emptyList();
        }
        Function<ADBEntity, Comparable<Object>> getter = data.get(0).getGetterForField(this.field);
        List<ADBComparable2IntPair> materialized = new ArrayList<>(this.sortedIndices.length);
        for(int sortedIndex : this.sortedIndices) {
            materialized.add(new ADBComparable2IntPair(getter.apply(data.get(sortedIndex)), data.get(sortedIndex).getInternalID()));
        }
        return materialized;
    }
}
