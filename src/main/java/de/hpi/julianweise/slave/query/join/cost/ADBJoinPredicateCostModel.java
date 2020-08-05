package de.hpi.julianweise.slave.query.join.cost;

import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.ADBPartialJoinResult;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@AllArgsConstructor
@Builder
@Getter
public class ADBJoinPredicateCostModel {
    private final ADBJoinQueryPredicate predicate;
    private final ADBInterval[][] joinCandidates;
    private final int originalSizeLeft;
    private final int originalSizeRight;

    public int getCost() {
        int cost = 0;
        for (ADBInterval[] rowIntervals : this.joinCandidates) {
            for (ADBInterval rowInterval : rowIntervals) {
                cost += rowInterval.size();
            }
        }
        return cost;
    }

    public double getRelativeCost() {
        return  ((double) this.getCost()) / (this.originalSizeLeft * this.originalSizeRight);
    }

    public ADBPartialJoinResult getJoinCandidates(Map<String, ObjectList<ADBEntityEntry>> left,
                                                  Map<String, ObjectList<ADBEntityEntry>> right) {
        ObjectList<ADBEntityEntry> leftValues = left.get(this.predicate.getLeftHandSideAttribute());
        ObjectList<ADBEntityEntry> rightValues = right.get(this.predicate.getRightHandSideAttribute());
        ADBPartialJoinResult candidates = new ADBPartialJoinResult(this.getCost());
        for (int i = 0; i < this.joinCandidates.length; i++) {
            for (ADBInterval interval : this.joinCandidates[i]) {
                candidates.addAllResults(this.getJoinCandidatesForRow(interval, leftValues.get(i), rightValues));
            }
        }
        return candidates;
    }

    public ADBPartialJoinResult getJoinCandidatesForRow(ADBInterval interval,
                                                        ADBEntityEntry left,
                                                        ObjectList<ADBEntityEntry> right) {
        ADBPartialJoinResult candidates = new ADBPartialJoinResult(interval.size());
        for (int i = interval.getStart(); i <= interval.getEnd(); i++) {
            candidates.addResult(left.getId(), right.get(i).getId());
        }
        return candidates;
    }

    @Override
    public String toString() {
        return String.format("[PredicateCostModel] for %s relCost: %f abs: %d origL: %d origR: %d",
                getPredicate(), getRelativeCost(), getCost(), getOriginalSizeLeft(), getOriginalSizeRight());
    }
}
