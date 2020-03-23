package de.hpi.julianweise.shard.queryOperation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBPair;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ADBJoinAttributeComparator extends AbstractBehavior<ADBJoinAttributeComparator.Command> {

    public interface Command extends CborSerializable {
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Compare implements Command {
        private int startIndexSourceAttributeValues;
        private int endIndexSourceAttributeValues;
        private ADBJoinQueryTerm term;
        Map<String, ADBSortedEntityAttributes> targetAttributeValues;
        private List<ADBPair<Comparable<?>, Integer>> sourceAttributeValues;
        private ActorRef<ADBLocalCompareAttributesSession.Command> respondTo;
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
        ArrayList<Pair<Integer, Integer>> joinPartners = new ArrayList<>();
        ADBSortedEntityAttributes targetVals = command.targetAttributeValues.get(command.term.getTargetAttributeName());
        for (int a = command.startIndexSourceAttributeValues; a < command.endIndexSourceAttributeValues; a++) {
            Comparable<Object> sourceAttributeVal = (Comparable<Object>) command.sourceAttributeValues.get(a).getKey();
            for (int b = 0; b < targetVals.size(); b++) {
                Comparable<?> targetAttributeVal = targetVals.get(b);
                if (ADBEntityType.matches(sourceAttributeVal, targetAttributeVal, command.term.getOperator())) {
                    joinPartners.add(new Pair<>(command.sourceAttributeValues.get(a).getValue(), targetVals.getOriginalIndex(b)));
                }
            }
        }
        command.respondTo.tell(ADBLocalCompareAttributesSession.HandleResults
                .builder()
                .term(command.term)
                .joinPartners(joinPartners)
                .build());
        return Behaviors.same();
    }
}
