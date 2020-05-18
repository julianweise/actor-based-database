package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinTermCostModel;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Builder
public class JoinQueryRowWorkload extends Workload {

    private final List<ADBKeyPair> joinCandidates;
    private final List<Map<String, ADBComparable2IntPair>> left;
    private final List<Map<String, ADBComparable2IntPair>> right;
    private final List<ADBJoinTermCostModel> costModels;

    @AllArgsConstructor
    @Getter
    public static class Results implements GenericWorker.Response {
        private final List<ADBKeyPair> results;
    }

    @Override
    protected void doExecute(GenericWorker.WorkloadMessage message) {
        List<ADBKeyPair> results = new ObjectArrayList<>();
        for (ADBKeyPair joinCandidate : this.joinCandidates) {
            if (this.rowSatisfyJoinCondition(joinCandidate)) {
                results.add(new ADBKeyPair(
                        left.get(ADBInternalIDHelper.getEntityId(joinCandidate.getKey()))
                            .get(this.costModels.get(0).getTerm().getLeftHandSideAttribute()).getValue(),
                        right.get(ADBInternalIDHelper.getEntityId(joinCandidate.getValue()))
                             .get(this.costModels.get(0).getTerm().getRightHandSideAttribute()).getValue()
                ));
            }
        }
        message.getRespondTo().tell(new Results(results));
    }

    private boolean rowSatisfyJoinCondition(ADBKeyPair candidate) {
        for (ADBJoinTermCostModel termCostModel : costModels) {
            val lField = left.get(ADBInternalIDHelper.getEntityId(candidate.getKey()))
                             .get(termCostModel.getTerm().getLeftHandSideAttribute()).getKey();
            val rField = right.get(ADBInternalIDHelper.getEntityId(candidate.getValue()))
                              .get(termCostModel.getTerm().getRightHandSideAttribute()).getKey();
            if (!ADBEntity.matches(lField, rField, termCostModel.getTerm().getOperator())) {
                return false;
            }
        }
        return true;
    }
}
