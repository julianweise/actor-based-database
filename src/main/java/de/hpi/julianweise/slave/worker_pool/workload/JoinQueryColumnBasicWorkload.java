package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.query.join.attribute_comparison.strategies.ADBAttributeComparisonStrategy;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

import java.util.List;

@Builder
public class JoinQueryColumnBasicWorkload extends Workload {

    public static final float JOIN_RESULT_REDUCTION_FACTOR = 0.3f;

    private final ADBQueryTerm.RelationalOperator operator;
    private final List<ADBComparable2IntPair> leftSideValues;
    private final List<ADBComparable2IntPair> rightSideValues;
    private final ADBAttributeComparisonStrategy strategy;

    @AllArgsConstructor
    @Getter
    public static class Results implements GenericWorker.Response {
        private final List<ADBKeyPair> results;
    }

    @Override
    public void doExecute(GenericWorker.WorkloadMessage message) {
        int resultSize = this.estimateResultSize(this.leftSideValues, this.rightSideValues);
        val joinTuples = this.strategy.compare(this.operator, this.leftSideValues, this.rightSideValues, resultSize);

        message.getRespondTo().tell(new Results(joinTuples));
    }

    private int estimateResultSize(List<ADBComparable2IntPair> l,
                                   List<ADBComparable2IntPair> r) {
        return Math.round(l.size() * r.size() * JOIN_RESULT_REDUCTION_FACTOR);
    }
}
