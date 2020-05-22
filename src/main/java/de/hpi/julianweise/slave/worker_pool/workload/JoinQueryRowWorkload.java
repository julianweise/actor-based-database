package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinTermCostModel;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

import java.util.Map;

@AllArgsConstructor
@Builder
public class JoinQueryRowWorkload extends Workload {

    private final ObjectList<ADBKeyPair> joinCandidates;
    private final ObjectList<Map<String, ADBComparable2IntPair>> left;
    private final ObjectList<Map<String, ADBComparable2IntPair>> right;
    private final ObjectList<ADBJoinTermCostModel> costModels;

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
                            .get(this.costModels.get(0).getPredicate().getLeftHandSideAttribute()).getValue(),
                        right.get(ADBInternalIDHelper.getEntityId(joinCandidate.getValue()))
                             .get(this.costModels.get(0).getPredicate().getRightHandSideAttribute()).getValue()
                ));
            }
        }
        message.getRespondTo().tell(new Results(results));
    }

    private boolean rowSatisfyJoinCondition(ADBKeyPair candidate) {
        for (ADBJoinTermCostModel termCostModel : costModels) {
            val lField = left.get(ADBInternalIDHelper.getEntityId(candidate.getKey()))
                             .get(termCostModel.getPredicate().getLeftHandSideAttribute()).getKey();
            val rField = right.get(ADBInternalIDHelper.getEntityId(candidate.getValue()))
                              .get(termCostModel.getPredicate().getRightHandSideAttribute()).getKey();
            if (!ADBEntity.matches(lField, rField, termCostModel.getPredicate().getOperator())) {
                return false;
            }
        }
        return true;
    }
}
