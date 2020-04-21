package de.hpi.julianweise.shard.query_operation.join.attribute_comparison;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.benchmarking.ADBQueryPerformanceSampler;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.shard.query_operation.join.ADBJoinTermComparator;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.strategies.ADBAttributeComparisonStrategy;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.util.List;

public class ADBJoinAttributeComparator extends AbstractBehavior<ADBJoinAttributeComparator.Command> {

    public static final float JOIN_RESULT_REDUCTION_FACTOR = 0.3f;
    private final ADBAttributeComparisonStrategy strategy;

    public interface Command extends CborSerializable {
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Compare implements Command {
        private ADBQueryTerm.RelationalOperator operator;
        private List<ADBPair<Comparable<?>, Integer>> leftSideValues;
        private List<ADBPair<Comparable<?>, Integer>> rightSideValues;
        private ActorRef<ADBJoinTermComparator.Command> respondTo;
    }

    public ADBJoinAttributeComparator(ActorContext<Command> context, ADBAttributeComparisonStrategy strategy) {
        super(context);
        this.strategy = strategy;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Compare.class, this::handleCompare)
                .build();
    }

    private Behavior<Command> handleCompare(Compare command) {
        ADBQueryPerformanceSampler.log(true, this.getClass().getSimpleName(), "Compare attributes");
        int resultSize = this.estimateResultSize(command.leftSideValues, command.rightSideValues);
        List<ADBKeyPair> joinTuples = this.strategy.compare(command.operator, command.leftSideValues,
                command.rightSideValues, resultSize);

        command.respondTo.tell(new ADBJoinTermComparator.CompareAttributesChunkResult(joinTuples));
        ADBQueryPerformanceSampler.log(false, this.getClass().getSimpleName(), "Compare attributes");
        return Behaviors.same();
    }

    private int estimateResultSize(List<ADBPair<Comparable<?>, Integer>> l, List<ADBPair<Comparable<?>, Integer>> r) {
        return Math.round(l.size() * r.size() * JOIN_RESULT_REDUCTION_FACTOR);
    }
}
