package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.ADBPartialJoinResult;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinPredicateCostModel;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@AllArgsConstructor
@Builder
public class JoinQueryRowWorkload extends Workload {

    private final ADBPartialJoinResult joinCandidates;
    private final ObjectList<Map<String, ADBEntityEntry>> left;
    private final ObjectList<Map<String, ADBEntityEntry>> right;
    private final ObjectList<ADBJoinPredicateCostModel> costModels;

    @AllArgsConstructor
    @Getter
    public static class Results implements GenericWorker.Response {
        private final ADBPartialJoinResult results;
    }

    @Override
    protected void doExecute(GenericWorker.WorkloadMessage message) {
        ADBPartialJoinResult results = new ADBPartialJoinResult();
        for (ADBKeyPair keyPair : this.joinCandidates) {
            if (this.rowSatisfyJoinCondition(keyPair.getKey(), keyPair.getValue())) {
                results.addResult(
                        left.get(ADBInternalIDHelper.getEntityId(keyPair.getKey()))
                            .get(this.costModels.get(0).getPredicate().getLeftHandSideAttribute()).getId(),
                        right.get(ADBInternalIDHelper.getEntityId(keyPair.getValue()))
                             .get(this.costModels.get(0).getPredicate().getRightHandSideAttribute()).getId()
                );
            }
        }
        message.getRespondTo().tell(new Results(results));
    }

    private boolean rowSatisfyJoinCondition(int leftId, int rightId) {
        for (ADBJoinPredicateCostModel termCostModel : costModels) {
            ADBEntityEntry lField = left.get(ADBInternalIDHelper.getEntityId(leftId))
                             .get(termCostModel.getPredicate().getLeftHandSideAttribute());
            ADBEntityEntry rField = right.get(ADBInternalIDHelper.getEntityId(rightId))
                              .get(termCostModel.getPredicate().getRightHandSideAttribute());
            if (lField == null || rField == null) {
                return false;
            }
            if (!ADBEntityEntry.matches(lField, rField, termCostModel.getPredicate().getOperator())) {
                return false;
            }
        }
        return true;
    }
}
