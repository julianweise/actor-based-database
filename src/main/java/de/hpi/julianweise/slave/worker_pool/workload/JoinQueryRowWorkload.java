package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinPredicateCostModel;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@AllArgsConstructor
@Builder
public class JoinQueryRowWorkload extends Workload {

    private final ObjectList<ADBKeyPair> joinCandidates;
    private final ObjectList<Map<String, ADBEntityEntry>> left;
    private final ObjectList<Map<String, ADBEntityEntry>> right;
    private final ObjectList<ADBJoinPredicateCostModel> costModels;

    @AllArgsConstructor
    @Getter
    public static class Results implements GenericWorker.Response {
        private final ObjectList<ADBKeyPair> results;
    }

    @Override
    protected void doExecute(GenericWorker.WorkloadMessage message) {
        ObjectList<ADBKeyPair> results = new ObjectArrayList<>();
        for (ADBKeyPair joinCandidate : this.joinCandidates) {
            if (this.rowSatisfyJoinCondition(joinCandidate)) {
                results.add(new ADBKeyPair(
                        left.get(ADBInternalIDHelper.getEntityId(joinCandidate.getKey()))
                            .get(this.costModels.get(0).getPredicate().getLeftHandSideAttribute()).getId(),
                        right.get(ADBInternalIDHelper.getEntityId(joinCandidate.getValue()))
                             .get(this.costModels.get(0).getPredicate().getRightHandSideAttribute()).getId()
                ));
            }
        }
        message.getRespondTo().tell(new Results(results));
    }

    private boolean rowSatisfyJoinCondition(ADBKeyPair candidate) {
        for (ADBJoinPredicateCostModel termCostModel : costModels) {
            ADBEntityEntry lField = left.get(ADBInternalIDHelper.getEntityId(candidate.getKey()))
                             .get(termCostModel.getPredicate().getLeftHandSideAttribute());
            ADBEntityEntry rField = right.get(ADBInternalIDHelper.getEntityId(candidate.getValue()))
                              .get(termCostModel.getPredicate().getRightHandSideAttribute());
            if (!ADBEntityEntry.matches(lField, rField, termCostModel.getPredicate().getOperator())) {
                return false;
            }
        }
        return true;
    }
}
