package de.hpi.julianweise.slave.worker_pool.workload;

import com.sun.tools.corba.se.idl.constExpr.Not;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
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
import org.agrona.collections.Object2ObjectHashMap;

import java.util.Map;

@AllArgsConstructor
@Builder
public class JoinQueryRowWorkload extends Workload {

    private final ADBPartialJoinResult joinCandidates;
    private final Map<String, ADBColumnSorted> left;
    private final Map<String, ADBColumnSorted> right;
    private final ObjectList<ADBJoinPredicateCostModel> costModels;
    private final Map<ADBJoinQueryPredicate, ADBComparator> comparators = new Object2ObjectHashMap<>();


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
                results.addResult(keyPair.getKey(), keyPair.getValue());
            }
        }
        message.getRespondTo().tell(new Results(results));
    }

    private boolean rowSatisfyJoinCondition(int leftId, int rightId) {
        assert ADBInternalIDHelper.getNodeId(leftId) == ADBInternalIDHelper.getNodeId(rightId): "Origin nodes differ!";
        assert ADBInternalIDHelper.getPartitionId(leftId) == ADBInternalIDHelper.getPartitionId(rightId): "Origin partitions differ!";
        for (ADBJoinPredicateCostModel termCostModel : costModels) {
            try {
                ADBEntityEntry left = this.left.get(termCostModel.getPredicate().getLeftHandSideAttribute())
                                               .getByOriginalIndex(ADBInternalIDHelper.getEntityId(leftId));
                ADBEntityEntry right = this.right.get(termCostModel.getPredicate().getRightHandSideAttribute())
                                                 .getByOriginalIndex(ADBInternalIDHelper.getEntityId(rightId));
                comparators.putIfAbsent(termCostModel.getPredicate(), ADBComparator.getFor(left.getValueField(),
                        right.getValueField()));
                if (!ADBEntityEntry.matches(left, right, termCostModel.getPredicate().getOperator(),
                        comparators.get(termCostModel.getPredicate()))) {
                    return false;
                }
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
        return true;
    }
}
