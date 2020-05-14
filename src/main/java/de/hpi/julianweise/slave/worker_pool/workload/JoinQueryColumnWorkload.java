package de.hpi.julianweise.slave.worker_pool.workload;

import com.zaxxer.sparsebits.SparseBitSet;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinTermCostModel;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInverseInterval;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

public class JoinQueryColumnWorkload extends Workload {

    public static final float JOIN_RESULT_REDUCTION_FACTOR = 0.3f;

    private final List<ADBPair<Comparable<Object>, Integer>> leftSideValues;
    private final List<ADBPair<Comparable<Object>, Integer>> rightSideValues;
    private final ADBJoinTermCostModel costModel;
    private final SparseBitSet[] bitMatrix;

    @AllArgsConstructor
    @Getter
    public static class Results implements GenericWorker.Response {
        private final SparseBitSet[] results;

    }

    @Builder
    public JoinQueryColumnWorkload(List<ADBPair<Comparable<Object>, Integer>> left,
                                   List<ADBPair<Comparable<Object>, Integer>> right,
                                   ADBJoinTermCostModel costModel) {
        this.leftSideValues = left;
        this.rightSideValues = right;
        this.costModel = costModel;
        this.bitMatrix = new SparseBitSet[this.leftSideValues.size()];
    }

    @Override
    public void doExecute(GenericWorker.WorkloadMessage message) {
        for(int i = 0; i < this.leftSideValues.size(); i++) {
            this.bitMatrix[i] = new SparseBitSet(this.rightSideValues.size());
        }
        for (int i = 0; i < this.costModel.getJoinCandidates().length; i++) {
            ADBInterval interval = this.costModel.getJoinCandidates()[i];
            if (interval instanceof ADBIntervalImpl) {
                this.handleInterval(i, (ADBIntervalImpl) interval);
            } else if (interval instanceof ADBInverseInterval) {
                this.handleInterval(i, (ADBInverseInterval) interval);
            }
        }
        message.getRespondTo().tell(new Results(this.bitMatrix));
    }

    private void handleInterval(int rowIndex, ADBIntervalImpl interval) {
        if (interval.equals(ADBIntervalImpl.NO_INTERSECTION)) {
            return;
        }
        this.handleInterval(rowIndex, interval.getStart(), interval.getEnd());
    }

    private void handleInterval(int rowIndex, ADBInverseInterval interval) {
        if (interval.equals(ADBInverseInterval.NO_INTERSECTION)) {
            return;
        }
        SparseBitSet bitSet = new SparseBitSet(this.rightSideValues.size());
        if (interval.getStart() > 0) {
            this.handleInterval(rowIndex, 0, interval.getStart() - 1);
        }
        if (interval.getEnd() < interval.getReferenceEnd()) {
            bitSet.set(interval.getEnd() + 1, interval.getReferenceEnd());
        }
    }

    private void handleInterval(int rowIndex, int start, int end) {
        for(int i = start; i <= end; i++) {
            int normalizedMatrixRow = ADBInternalIDHelper.getEntityId(this.leftSideValues.get(rowIndex).getValue());
            int normalizedMatrixColumn = ADBInternalIDHelper.getEntityId(this.rightSideValues.get(i).getValue());
            this.bitMatrix[normalizedMatrixRow].set(normalizedMatrixColumn);
        }
    }

}