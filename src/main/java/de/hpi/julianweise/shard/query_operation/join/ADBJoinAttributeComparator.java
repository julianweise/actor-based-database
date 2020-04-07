package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

public class ADBJoinAttributeComparator extends AbstractBehavior<ADBJoinAttributeComparator.Command> {

    public static final float JOIN_RESULT_REDUCTION_FACTOR = 0.3f;

    public interface Command extends CborSerializable {}

    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Compare implements Command {
        private ADBQueryTerm.RelationalOperator operator;
        private List<ADBPair<Comparable<?>, Integer>> leftSideValues;
        private List<ADBPair<Comparable<?>, Integer>> rightSideValues;
        private ActorRef<ADBJoinTermComparator.Command> respondTo;
    }

    public ADBJoinAttributeComparator(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Compare.class, this::handleCompare)
                .build();
    }

    @SuppressWarnings("unchecked")
    private Behavior<Command> handleCompare(Compare command) {
        int estimatedResultSize = this.estimateResultSize(command.leftSideValues, command.rightSideValues);
        ArrayList<ADBKeyPair> joinCandidates = new ArrayList<>(estimatedResultSize);

        for (ADBPair<Comparable<?>, Integer> leftValue : command.leftSideValues) {
            for (ADBPair<Comparable<?>, Integer> rightValue : command.rightSideValues) {
                if (ADBEntityType.matches((Comparable<Object>) leftValue.getKey(), rightValue.getKey(), command.operator)) {
                    joinCandidates.add(new ADBKeyPair(leftValue.getValue(), rightValue.getValue()));
                }
            }
        }

        joinCandidates.trimToSize();
        command.respondTo.tell(new ADBJoinTermComparator.CompareAttributesChunkResult(joinCandidates));
        return Behaviors.same();
    }

    private int estimateResultSize(List<ADBPair<Comparable<?>, Integer>> l, List<ADBPair<Comparable<?>, Integer>> r) {
        return Math.round(l.size() * r.size() * JOIN_RESULT_REDUCTION_FACTOR);
    }
}
