package de.hpi.julianweise.slave.query.join.cost;

import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInverseInterval;
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
public class ADBJoinTermCostModel {
    private final ADBJoinQueryPredicate predicate;
    private final ADBInterval[] joinCandidates;
    private final int sizeLeft;
    private final int sizeRight;
    private final int termId;

    public int getCost() {
        return Arrays.stream(this.joinCandidates).mapToInt(ADBInterval::size).sum();
    }

    public float getRelativeCost() {
        return (float) this.getCost() / (this.sizeLeft * this.sizeRight);
    }

    public ObjectList<ADBKeyPair> getJoinCandidates(Map<String, ObjectList<ADBEntityEntry>> left,
                                                    Map<String, ObjectList<ADBEntityEntry>> right) {
        ObjectList<ADBEntityEntry> leftValues = left.get(this.predicate.getLeftHandSideAttribute());
        ObjectList<ADBEntityEntry> rightValues = right.get(this.predicate.getRightHandSideAttribute());
        ObjectList<ADBKeyPair> candidates = new ObjectArrayList<>(this.getCost());
        for (int i = 0; i < this.joinCandidates.length; i++) {
            ADBInterval interval = this.joinCandidates[i];
            if (interval instanceof ADBIntervalImpl) {
                candidates.addAll(getJoinCandidatesForRow((ADBIntervalImpl) interval, leftValues.get(i), rightValues));
            } else if(interval instanceof ADBInverseInterval) {
                candidates.addAll(this.getJoinCandidatesForRow((ADBInverseInterval) interval, leftValues.get(i), rightValues));
            }
        }
        return candidates;
    }

    public ObjectList<ADBKeyPair> getJoinCandidatesForRow(ADBIntervalImpl interval,
                                                    ADBEntityEntry left,
                                                    ObjectList<ADBEntityEntry> right) {
        if (interval.equals(ADBIntervalImpl.NO_INTERSECTION)) {
            return new ObjectArrayList<>();
        }
        ObjectList<ADBKeyPair> candidates = new ObjectArrayList<>(interval.size());
        for(int i = interval.getStart(); i <= interval.getEnd(); i++) {
            candidates.add(new ADBKeyPair(left.getId(), right.get(i).getId()));
        }
        return candidates;
    }

    public ObjectList<ADBKeyPair> getJoinCandidatesForRow(ADBInverseInterval interval,
                                                          ADBEntityEntry left,
                                                    ObjectList<ADBEntityEntry> right) {
        if (interval.equals(ADBInverseInterval.NO_INTERSECTION)) {
            return new ObjectArrayList<>();
        }
        ObjectList<ADBKeyPair> candidates = new ObjectArrayList<>(interval.size());
        if (interval.getStart() > 0) {
            for(int i = 0; i < interval.getStart(); i++) {
                candidates.add(new ADBKeyPair(left.getId(), right.get(i).getId()));
            }
        }
        if (interval.getEnd() < interval.getReferenceEnd()) {
            for(int i = interval.getEnd(); i <= interval.getReferenceEnd(); i++) {
                candidates.add(new ADBKeyPair(left.getId(), right.get(i).getId()));
            }
        }
        return candidates;
    }

    @Override
    public String toString() {
        return "[Predicate Cost Model] for: " + getPredicate() + " relCost: " + getRelativeCost() + " abs: " + getCost();
    }
}
