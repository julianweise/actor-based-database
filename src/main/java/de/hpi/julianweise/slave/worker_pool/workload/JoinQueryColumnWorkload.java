package de.hpi.julianweise.slave.worker_pool.workload;

import com.zaxxer.sparsebits.SparseBitSet;
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
                                   ADBJoinPredicateCostModel costModel) {
        this.leftSideValues = left;
        this.rightSideValues = right;
        this.costModel = costModel;
        // Information could also be transferred from origin-partition
        int maxId = this.leftSideValues.stream()
                                       .mapToInt(e -> ADBInternalIDHelper.getEntityId(e.getId()))
                                       .max().orElse(this.leftSideValues.size());
        this.bitMatrix = new SparseBitSet[maxId + 1];
    }

    @Override
    public void doExecute(GenericWorker.WorkloadMessage message) {
        int maxIdRight = this.rightSideValues.stream()
                                       .mapToInt(e -> ADBInternalIDHelper.getEntityId(e.getId()))
                                       .max().orElse(this.rightSideValues.size());
        for(int i = 0; i < this.bitMatrix.length; i++) {
            this.bitMatrix[i] = new SparseBitSet(maxIdRight);
        }
        for (int i = 0; i < this.costModel.getJoinCandidates().length; i++) {
            for (ADBInterval interval : this.costModel.getJoinCandidates()[i]) {
                this.handleInterval(i, interval);
            }
        }
        message.getRespondTo().tell(new Results(this.bitMatrix));
    }

    private void handleInterval(int rowIndex, ADBInterval interval) {
        if (interval.equals(ADBInterval.NO_INTERSECTION)) {
            return;
        }
        this.handleInterval(rowIndex, interval.getStart(), interval.getEnd());
    }

    private void handleInterval(int rowIndex, int start, int end) {
        for(int i = start; i <= end; i++) {
            int normalizedMatrixRow = ADBInternalIDHelper.getEntityId(this.leftSideValues.get(rowIndex).getId());
            int normalizedMatrixColumn = ADBInternalIDHelper.getEntityId(this.rightSideValues.get(i).getId());
            this.bitMatrix[normalizedMatrixRow].set(normalizedMatrixColumn);
        }
    }

}
