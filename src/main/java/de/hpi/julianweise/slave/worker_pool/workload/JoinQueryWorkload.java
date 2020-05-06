package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.query.join.attribute_comparison.strategies.ADBAttributeComparisonStrategy;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

import java.util.List;

@Builder
public class JoinQueryWorkload extends Workload {

    public static final float JOIN_RESULT_REDUCTION_FACTOR = 0.3f;

    private final ADBQueryTerm.RelationalOperator operator;
    private final List<ADBPair<Comparable<Object>, Integer>> leftSideValues;
    private final List<ADBPair<Comparable<Object>, Integer>> rightSideValues;
    private final ADBAttributeComparisonStrategy strategy;
    private final int foreignPartitionId;

    @AllArgsConstructor
    @Getter
    public static class Results implements GenericWorker.Response {
        private final int foreignPartitionId;
        private final List<ADBKeyPair> results;
    }

    @Override
    public void doExecute(GenericWorker.WorkloadMessage message) {
        int resultSize = this.estimateResultSize(this.leftSideValues, this.rightSideValues);
        val joinTuples = this.strategy.compare(this.operator, this.leftSideValues, this.rightSideValues, resultSize);

        message.getRespondTo().tell(new Results(this.foreignPartitionId, joinTuples));
    }

    private int estimateResultSize(List<ADBPair<Comparable<Object>, Integer>> l,
                                   List<ADBPair<Comparable<Object>, Integer>> r) {
        return Math.round(l.size() * r.size() * JOIN_RESULT_REDUCTION_FACTOR);
    }
}
