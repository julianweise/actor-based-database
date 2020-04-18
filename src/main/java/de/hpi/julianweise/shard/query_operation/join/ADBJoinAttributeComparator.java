package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.ADBOffsetCalculator;
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

    public ADBJoinAttributeComparator(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Compare.class, this::handleCompare)
                .build();
    }

    private Behavior<Command> handleCompare(Compare command) {
        List<ADBPair<Comparable<?>, Integer>> left = command.leftSideValues;
        List<ADBPair<Comparable<?>, Integer>> right = command.rightSideValues;
        int resultSize = this.estimateResultSize(left, right);
        int[] offset = ADBOffsetCalculator.calc(left, right);
        ArrayList<ADBKeyPair> joinTuples;

        switch (command.operator) {
            case GREATER: joinTuples = this.compareGreater(left, right, offset, resultSize); break;
            case GREATER_OR_EQUAL: joinTuples = this.compareGreaterEquals(left, right, offset, resultSize); break;
            case LESS: joinTuples = this.compareLess(left, right, offset, resultSize); break;
            case LESS_OR_EQUAL: joinTuples = this.compareLessEqual(left, right, offset, resultSize); break;
            case EQUALITY: joinTuples = this.compareEqual(left, right, offset, resultSize); break;
            default: throw new IllegalArgumentException("Operator " + command.operator + " is not supported." );
        }
        joinTuples.trimToSize();
        command.respondTo.tell(new ADBJoinTermComparator.CompareAttributesChunkResult(joinTuples));
        return Behaviors.same();
    }

    @SuppressWarnings("unchecked")
    private ArrayList<ADBKeyPair> compareGreater(List<ADBPair<Comparable<?>, Integer>> left,
                                                 List<ADBPair<Comparable<?>, Integer>> right,
                                                 int[] offset,
                                                 int estimatedResultSize) {
        ArrayList<ADBKeyPair> joinTuples = new ArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            int offsetCorrection =
                    ((Comparable<Object>)left.get(a).getKey()).compareTo(right.get(offset[a]).getKey()) < 0 ? -1 : 0;
            for (int b = offset[a] + offsetCorrection; b > -1; b--) {
                if (left.get(a).getKey().equals(right.get(b).getKey())) {
                    continue;
                }
                joinTuples.add(new ADBKeyPair(left.get(a).getValue(), right.get(b).getValue()));
            }
        }
        return joinTuples;
    }

    @SuppressWarnings("unchecked")
    private ArrayList<ADBKeyPair> compareGreaterEquals(List<ADBPair<Comparable<?>, Integer>> left,
                                                       List<ADBPair<Comparable<?>, Integer>> right,
                                                       int[] offset,
                                                       int estimatedResultSize) {
        ArrayList<ADBKeyPair> joinTuples = new ArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            int offsetCorrection =
                    ((Comparable<Object>)left.get(a).getKey()).compareTo(right.get(offset[a]).getKey()) < 0 ? -1 : 0;
            for (int b = offset[a] + offsetCorrection; b > -1; b--) {
                joinTuples.add(new ADBKeyPair(left.get(a).getValue(), right.get(b).getValue()));
            }
        }
        return joinTuples;
    }

    private ArrayList<ADBKeyPair> compareLess(List<ADBPair<Comparable<?>, Integer>> left,
                                              List<ADBPair<Comparable<?>, Integer>> right,
                                              int[] offset,
                                              int estimatedResultSize) {
        ArrayList<ADBKeyPair> joinTuples = new ArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            int offsetCorrection = left.get(a).getKey().equals(right.get(offset[a]).getKey()) ? 1 : 0;
            for (int b = offset[a] + offsetCorrection; b > -1 && b < right.size(); b++) {
                joinTuples.add(new ADBKeyPair(left.get(a).getValue(), right.get(b).getValue()));
            }
        }
        return joinTuples;
    }

    private ArrayList<ADBKeyPair> compareLessEqual(List<ADBPair<Comparable<?>, Integer>> left,
                                                   List<ADBPair<Comparable<?>, Integer>> right,
                                                   int[] offset,
                                                   int estimatedResultSize) {
        ArrayList<ADBKeyPair> joinTuples = new ArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            int offsetCorrection = 0;
            while (offset[a] - offsetCorrection - 1 > 0 && left.get(a).getKey().equals(right.get(offset[a] - offsetCorrection - 1).getKey())) offsetCorrection--;
            for (int b = offset[a] + offsetCorrection; b > -1 && b < right.size(); b++) {
                joinTuples.add(new ADBKeyPair(left.get(a).getValue(), right.get(b).getValue()));
            }
        }
        return joinTuples;
    }

    private ArrayList<ADBKeyPair> compareEqual(List<ADBPair<Comparable<?>, Integer>> left,
                                                   List<ADBPair<Comparable<?>, Integer>> right,
                                                   int[] offset,
                                                   int estimatedResultSize) {
        ArrayList<ADBKeyPair> joinTuples = new ArrayList<>(estimatedResultSize);
        for (int a = 0; a < left.size(); a++) {
            for (int b = offset[a]; b > -1 && left.get(a).getKey().equals(right.get(b).getKey()); b--) {
                joinTuples.add(new ADBKeyPair(left.get(a).getValue(), right.get(b).getValue()));
            }
        }
        return joinTuples;
    }

    private int estimateResultSize(List<ADBPair<Comparable<?>, Integer>> l, List<ADBPair<Comparable<?>, Integer>> r) {
        return Math.round(l.size() * r.size() * JOIN_RESULT_REDUCTION_FACTOR);
    }
}
