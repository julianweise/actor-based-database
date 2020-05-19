package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.function.Function;

@AllArgsConstructor
@Getter
public class ADBSortedEntityAttributes2 {

    private final String field;
    private final int[] sortedIndices;

    public ObjectList<ADBComparable2IntPair> getMaterialized(ObjectList<ADBEntity> data) {
        if (data.size() < 1) {
            return new ObjectArrayList<>();
        }
        Function<ADBEntity, Comparable<Object>> getter = data.get(0).getGetterForField(this.field);
        ObjectList<ADBComparable2IntPair> materialized = new ObjectArrayList<>(this.sortedIndices.length);
        for(int sortedIndex : this.sortedIndices) {
            materialized.add(new ADBComparable2IntPair(getter.apply(data.get(sortedIndex)), data.get(sortedIndex).getInternalID()));
        }
        return materialized;
    }
}
