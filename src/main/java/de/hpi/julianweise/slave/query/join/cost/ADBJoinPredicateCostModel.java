package de.hpi.julianweise.slave.query.join.cost;

import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;

@AllArgsConstructor
@Builder
@Getter
public class ADBJoinPredicateCostModel {
    private final ADBJoinQueryPredicate predicate;
    private final ADBInterval[][] joinCandidates;
    private final int sizeRight;

    public int getCost() {
        return Arrays.stream(this.joinCandidates).flatMap(Arrays::stream).mapToInt(ADBInterval::size).sum();
    }

    public float getRelativeCost() {
        return (float) this.getCost() / (this.getSizeLeft() * this.sizeRight);
    }

    public ObjectList<ADBKeyPair> getJoinCandidates(Map<String, ObjectList<ADBEntityEntry>> left,
                                                    Map<String, ObjectList<ADBEntityEntry>> right) {
        ObjectList<ADBEntityEntry> leftValues = left.get(this.predicate.getLeftHandSideAttribute());
        ObjectList<ADBEntityEntry> rightValues = right.get(this.predicate.getRightHandSideAttribute());
        ObjectList<ADBKeyPair> candidates = new ObjectArrayList<>(this.getCost());
        for (int i = 0; i < this.joinCandidates.length; i++) {
            for (ADBInterval interval : this.joinCandidates[i]) {
                candidates.addAll(getJoinCandidatesForRow(interval, leftValues.get(i), rightValues));
            }
        }
        return candidates;
    }

    public ObjectList<ADBKeyPair> getJoinCandidatesForRow(ADBInterval interval,
                                                          ADBEntityEntry left,
                                                          ObjectList<ADBEntityEntry> right) {
        if (interval.equals(ADBInterval.NO_INTERSECTION)) {
            return new ObjectArrayList<>();
        }
        ObjectList<ADBKeyPair> candidates = new ObjectArrayList<>(interval.size());
        for (int i = interval.getStart(); i <= interval.getEnd(); i++) {
            candidates.add(new ADBKeyPair(left.getId(), right.get(i).getId()));
        }
        return candidates;
    }

    public int getSizeLeft() {
        return this.joinCandidates.length;
    }

    @Override
    public String toString() {
        return "[PredicateCostModel] for: " + getPredicate() + " relCost: " + getRelativeCost() + " abs: " + getCost();
    }
}
