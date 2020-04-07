package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBJoinTermComparator extends AbstractBehavior<ADBJoinTermComparator.Command> {

    public static final int CHUNK_SIZE_COMPARISON = 2000;
    public static final float JOIN_RESULT_REDUCTION_FACTOR = 0.3f;

    private final ADBJoinQueryTerm term;
    private final AtomicInteger chunkCounter = new AtomicInteger(0);
    private List<ADBKeyPair> comparisonResults;
    private final ActorRef<ADBJoinQueryComparator.Command> supervisor;
    private final ActorRef<ADBJoinAttributeComparator.Command> comparatorPool;

    public interface Command {}
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class CompareTerm implements Command {
        private List<ADBPair<Comparable<?>, Integer>> leftValues;
        private List<ADBPair<Comparable<?>, Integer>> rightValues;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class CompareAttributesChunkResult implements Command {
        private List<ADBKeyPair> joinPartners;
    }


    public ADBJoinTermComparator(ActorContext<Command> context, ADBJoinQueryTerm term,
                                 ActorRef<ADBJoinQueryComparator.Command> supervisor,
                                 ActorRef<ADBJoinAttributeComparator.Command> comparatorPool) {
        super(context);
        this.term = term;
        this.supervisor = supervisor;
        this.comparatorPool = comparatorPool;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(CompareTerm.class, this::handleCompareTerm)
                .onMessage(CompareAttributesChunkResult.class, this::handleCompareAttributesChunkResults)
                .build();
    }

    private Behavior<Command> handleCompareTerm(CompareTerm command) {
        this.getContext().getLog().info("Comparing attributes for " + this.term);
        this.comparisonResults = new ArrayList<>(this.estimateResultSize(command.leftValues, command.rightValues));
        for (int i = 0; i <= command.rightValues.size(); i = i + CHUNK_SIZE_COMPARISON) {
            this.chunkCounter.incrementAndGet();
            int rightSideValuesEnd = Math.min(command.rightValues.size(), i + CHUNK_SIZE_COMPARISON);
            this.comparatorPool.tell(ADBJoinAttributeComparator.Compare
                    .builder()
                    .operator(this.term.getOperator())
                    .leftSideValues(command.leftValues)
                    .rightSideValues(command.rightValues.subList(i, rightSideValuesEnd))
                    .respondTo(this.getContext().getSelf())
                    .build());
        }
        return Behaviors.same();
    }

    private int estimateResultSize(List<ADBPair<Comparable<?>, Integer>> l, List<ADBPair<Comparable<?>, Integer>> r) {
        return Math.round(l.size() * r.size() * JOIN_RESULT_REDUCTION_FACTOR);
    }

    private Behavior<Command> handleCompareAttributesChunkResults(CompareAttributesChunkResult results) {
        this.comparisonResults.addAll(results.getJoinPartners());
        if (this.chunkCounter.decrementAndGet() < 1) {
            this.getContext().getLog().info("Finalized comparing attributes for " + this.term + ". Received " + this.comparisonResults.size() + " tuples.");
            this.supervisor.tell(new ADBJoinQueryComparator.CompareTermResults(this.comparisonResults, this.term));
            return Behaviors.stopped();
        }
        return Behaviors.same();
    }
}
