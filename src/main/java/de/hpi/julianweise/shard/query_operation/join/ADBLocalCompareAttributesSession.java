package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBLocalCompareAttributesSession extends AbstractBehavior<ADBLocalCompareAttributesSession.Command> {

    private final ActorRef<ADBJoinAttributeComparator.Command> comparatorPool;
    private final Map<String, ADBSortedEntityAttributes> sortedLocalJoinAttributes;
    private final ActorRef<ADBJoinWithShardSessionHandler.Command> respondTo;
    private final AtomicInteger processCounter = new AtomicInteger(0);
    private final Map<ADBJoinQueryTerm, List<Pair<Integer, Integer>>> intermediateJoinResults = new HashMap<>();

    public interface Command extends CborSerializable {
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @Getter
    public static class CompareJoinAttributes implements Command {
        private String sourceAttributeName;
        private List<ADBPair<Comparable<?>, Integer>> sourceAttributes;
        private ADBJoinQueryTerm[] terms;

    }
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @Getter
    public static class HandleResults implements ADBLocalCompareAttributesSession.Command {
        private ADBJoinQueryTerm term;
        private List<Pair<Integer, Integer>> joinPartners;

    }
    public static final int CHUNK_SIZE_COMPARISON = 2000;



    public ADBLocalCompareAttributesSession(ActorContext<Command> context,
                                            ActorRef<ADBJoinAttributeComparator.Command> comparatorPool,
                                            Map<String, ADBSortedEntityAttributes> sortedLocalJoinAttributes,
                                            ActorRef<ADBLargeMessageActor.Command> respondTo) {
        super(context);
        this.comparatorPool = comparatorPool;
        this.respondTo = respondTo;
        this.sortedLocalJoinAttributes = sortedLocalJoinAttributes;

        this.getContext().getLog().info("Starting ...");
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(CompareJoinAttributes.class, this::handleCompareJoinAttributes)
                .onMessage(HandleResults.class, this::handleResults)
                .build();
    }

    private Behavior<Command> handleCompareJoinAttributes(CompareJoinAttributes message) {
        this.getContext().getLog().info("Received " + message.terms.length + " terms to compare against "
                + message.sourceAttributeName);
        for (int i = 0; i < message.terms.length; i++) {
            this.intermediateJoinResults.putIfAbsent(message.terms[i], new ArrayList<>());
            this.processTerms(message.terms[i], message.sourceAttributes);
        }
        return Behaviors.same();
    }


    private void processTerms(ADBJoinQueryTerm term, List<ADBPair<Comparable<?>, Integer>> sourceAttributes) {
        for (int i = 0; i < (sourceAttributes.size() / CHUNK_SIZE_COMPARISON) + 1; i++) {
            this.processCounter.incrementAndGet();
            this.comparatorPool.tell(ADBJoinAttributeComparator.Compare
                    .builder()
                    .startIndexSourceAttributeValues(i * CHUNK_SIZE_COMPARISON)
                    .endIndexSourceAttributeValues(Math.min((i + 1) * CHUNK_SIZE_COMPARISON, sourceAttributes.size()))
                    .term(term)
                    .targetAttributeValues(this.sortedLocalJoinAttributes)
                    .sourceAttributeValues(sourceAttributes)
                    .respondTo(this.getContext().getSelf())
                    .build());
        }
    }

    private Behavior<Command> handleResults(HandleResults command) {
        this.intermediateJoinResults.get(command.term).addAll(command.joinPartners);
        if (this.processCounter.decrementAndGet() > 0) {
            return Behaviors.same();
        }
        this.getContext().getLog().info("Finalized comparisons for " + command.term.getSourceAttributeName());
        int i = 0;
        for(Map.Entry<ADBJoinQueryTerm, List<Pair<Integer, Integer>>> termResults :
                this.intermediateJoinResults.entrySet()) {
            this.respondTo.tell(ADBJoinWithShardSessionHandler.JoinAttributesComparedFor
                    .builder()
                    .joinCandidates(termResults.getValue())
                    .sourceAttributeName(termResults.getKey().getSourceAttributeName())
                    .isLastChunk(i == (this.intermediateJoinResults.size() - 1))
                    .build());
            i++;
        }
        return Behaviors.stopped();
    }

}
