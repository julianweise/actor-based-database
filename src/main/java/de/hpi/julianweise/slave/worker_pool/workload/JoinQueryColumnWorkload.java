package de.hpi.julianweise.slave.worker_pool.workload;

import com.zaxxer.sparsebits.SparseBitSet;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinPredicateCostModel;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

public class JoinQueryColumnWorkload extends Workload {

    private final ObjectList<ADBEntityEntry> leftSideValues;
    private final ObjectList<ADBEntityEntry> rightSideValues;
    private final int rightOriginalSize;
    private final ADBJoinPredicateCostModel costModel;
    private final SparseBitSet[] bitMatrix;

    @AllArgsConstructor
    @Getter
    public static class Results implements GenericWorker.Response {
        private final SparseBitSet[] results;
    }

    @SuppressWarnings("unused")
    @Builder
    public JoinQueryColumnWorkload(ObjectList<ADBEntityEntry> left,
                                   ObjectList<ADBEntityEntry> right,
                                   int leftOriginalSize,
                                   int rightOriginalSize,
                                   ADBJoinPredicateCostModel costModel) {
        this.leftSideValues = left;
        this.rightSideValues = right;
        this.costModel = costModel;
        this.rightOriginalSize = rightOriginalSize;
        this.bitMatrix = new SparseBitSet[leftOriginalSize];
    }

    @Override
    public void doExecute(GenericWorker.WorkloadMessage message) {
        this.buildMatrix();
        if (this.costModel.getPredicate().getOperator().equals(ADBQueryTerm.RelationalOperator.INEQUALITY)) {
            this.doExecutedMultipleIntervals();
        } else {
            this.doExecuteSingleInterval();
        }
        message.getRespondTo().tell(new Results(this.bitMatrix));
    }

    public void doExecuteSingleInterval() {
        for (int i = 0; i < this.costModel.getJoinCandidates().length; i++) {
            ADBInterval interval = this.costModel.getJoinCandidates()[i][0];
            this.handleInterval(i, interval.getStart(), interval.getEnd());
        }
    }

    public void doExecutedMultipleIntervals() {
        for (int i = 0; i < this.costModel.getJoinCandidates().length; i++) {
            for (ADBInterval interval : this.costModel.getJoinCandidates()[i]) {
                this.handleInterval(i, interval.getStart(), interval.getEnd());
            }
        }
    }

    private void buildMatrix() {
        for(int i = 0; i < this.bitMatrix.length; i++) {
            this.bitMatrix[i] = new SparseBitSet(this.rightOriginalSize);
        }
    }

    private void handleInterval(int rowIndex, int start, int end) {
        int normalizedMatrixRow = ADBInternalIDHelper.getEntityId(this.leftSideValues.get(rowIndex).getId());
        for(int i = start; i <= end; i++) {
            int normalizedMatrixColumn = ADBInternalIDHelper.getEntityId(this.rightSideValues.get(i).getId());
            this.bitMatrix[normalizedMatrixRow].set(normalizedMatrixColumn);
        }
    }

}
