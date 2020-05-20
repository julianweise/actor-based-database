package de.hpi.julianweise.slave.query.join.cost;

import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInverseInterval;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
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
    private final ADBJoinQueryTerm term;
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

    public ObjectList<ADBKeyPair> getJoinCandidates(Map<String, ObjectList<ADBComparable2IntPair>> left,
                                                    Map<String, ObjectList<ADBComparable2IntPair>> right) {
        ObjectList<ADBComparable2IntPair> leftValues = left.get(this.term.getLeftHandSideAttribute());
        ObjectList<ADBComparable2IntPair> rightValues = right.get(this.term.getRightHandSideAttribute());
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
                                                    ADBComparable2IntPair left,
                                                    ObjectList<ADBComparable2IntPair> right) {
        if (interval.equals(ADBIntervalImpl.NO_INTERSECTION)) {
            return new ObjectArrayList<>();
        }
        ObjectList<ADBKeyPair> candidates = new ObjectArrayList<>(interval.size());
        for(int i = interval.getStart(); i <= interval.getEnd(); i++) {
            candidates.add(new ADBKeyPair(left.getValue(), right.get(i).getValue()));
        }
        return candidates;
    }

    public ObjectList<ADBKeyPair> getJoinCandidatesForRow(ADBInverseInterval interval,
                                                    ADBComparable2IntPair left,
                                                    ObjectList<ADBComparable2IntPair> right) {
        if (interval.equals(ADBInverseInterval.NO_INTERSECTION)) {
            return new ObjectArrayList<>();
        }
        ObjectList<ADBKeyPair> candidates = new ObjectArrayList<>(interval.size());
        if (interval.getStart() > 0) {
            for(int i = 0; i < interval.getStart(); i++) {
                candidates.add(new ADBKeyPair(left.getValue(), right.get(i).getValue()));
            }
        }
        if (interval.getEnd() < interval.getReferenceEnd()) {
            for(int i = interval.getEnd(); i <= interval.getReferenceEnd(); i++) {
                candidates.add(new ADBKeyPair(left.getValue(), right.get(i).getValue()));
            }
        }
        return candidates;
    }

    @Override
    public String toString() {
        return "[Predicate Cost Model] for: " + getTerm() + " relCost: " + getRelativeCost() + " abs: " + getCost();
    }
}
