package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.attribute_comparison.strategies.ADBAttributeComparisonStrategy;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

@Builder
public class JoinQueryColumnBasicWorkload extends Workload {

    public static final float JOIN_RESULT_REDUCTION_FACTOR = 0.3f;

    private final ADBQueryTerm.RelationalOperator operator;
    private final ObjectList<ADBEntityEntry> leftSideValues;
    private final ObjectList<ADBEntityEntry> rightSideValues;
    private final ADBAttributeComparisonStrategy strategy;

    @AllArgsConstructor
    @Getter
    public static class Results implements GenericWorker.Response {
        private final ObjectList<ADBKeyPair> results;
    }

    @Override
    public void doExecute(GenericWorker.WorkloadMessage message) {
        int resultSize = this.estimateResultSize(this.leftSideValues, this.rightSideValues);
        val joinTuples = this.strategy.compare(this.operator, this.leftSideValues, this.rightSideValues, resultSize);

        message.getRespondTo().tell(new Results(joinTuples));
    }

    private int estimateResultSize(ObjectList<ADBEntityEntry> l,
                                   ObjectList<ADBEntityEntry> r) {
        return Math.round(l.size() * r.size() * JOIN_RESULT_REDUCTION_FACTOR);
    }
}
