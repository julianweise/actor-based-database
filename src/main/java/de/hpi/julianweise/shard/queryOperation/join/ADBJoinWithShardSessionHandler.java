package de.hpi.julianweise.shard.queryOperation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBLargeMessageSender;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBLargeMessageSenderFactory;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBPair;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ADBJoinWithShardSessionHandler extends ADBLargeMessageActor {

    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class CompareJoinAttributesFor implements Command, ADBLargeMessageSender.LargeMessage {
        String sourceAttribute;
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
        @JsonSubTypes({
                              @JsonSubTypes.Type(value = String.class, name = "String"),
                              @JsonSubTypes.Type(value = Integer.class, name = "Integer"),
                              @JsonSubTypes.Type(value = Float.class, name = "Float"),
                              @JsonSubTypes.Type(value = Double.class, name = "Double"),
                              @JsonSubTypes.Type(value = Character.class, name = "Character"),
                              @JsonSubTypes.Type(value = Boolean.class, name = "Boolean"),
                      })
        List<ADBPair<Comparable<?>, Integer>> sourceAttributes;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class JoinAttributesComparedFor implements Command {
        String sourceAttributeName;
        Set<Pair<Integer, Integer>> joinCandidates;
        boolean isLastChunk;
    }

    private final ActorRef<ADBJoinWithShardSession.Command> session;
    private final Map<String, ADBSortedEntityAttributes> sortedJoinAttributes;
    private final Map<String, List<ADBJoinQueryTerm>> groupedQueryTerms;
    private final ActorRef<ADBJoinAttributeComparator.Command> comparatorPool;
    private final Set<Pair<Integer, Integer>> joinCandidates = new HashSet<>();
    private final List<ADBEntityType> data;

    public ADBJoinWithShardSessionHandler(ActorContext<Command> context,
                                          ActorRef<ADBJoinWithShardSession.Command> session,
                                          ADBQuery query,
                                          Map<String, ADBSortedEntityAttributes> sortedJoinAttributes,
                                          List<ADBEntityType> data) {
        super(context);
        this.session = session;
        this.data = data;
        this.sortedJoinAttributes = sortedJoinAttributes;
        this.comparatorPool = ADBLocalCompareAttributeSessionFactory.getComparatorPool(this.getContext());
        this.groupedQueryTerms = query.getTerms()
                                      .stream()
                                      .map(term -> ((ADBJoinQueryTerm) term))
                                      .collect(Collectors.groupingBy(ADBJoinQueryTerm::getSourceAttributeName));
        session.tell(new ADBJoinWithShardSession.RegisterHandler(this.getContext().getSelf()));
        this.getContext().getLog().info("Start new SessionHandler for joining with " + this.session.path().name());

    }

    @Override
    public Receive<Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(CompareJoinAttributesFor.class, this::handleCompareJoinAttributes)
                   .onMessage(JoinAttributesComparedFor.class, this::handleJoinAttributesCompared)
                   .build();
    }


    private Behavior<Command> handleCompareJoinAttributes(CompareJoinAttributesFor command) {
        this.getContext().getLog().info("Received attributes for join comparison: " + command.sourceAttribute);
        ActorRef<ADBLocalCompareAttributesSession.Command> localJoinAttributeCompareSession =
                this.getContext().spawn(ADBLocalCompareAttributeSessionFactory.createDefault(this.comparatorPool,
                        this.sortedJoinAttributes, this.getContext().getSelf()),
                        "LocalJoinAttributeComparator-for-" + command.sourceAttribute.replace(" ", ""));

        localJoinAttributeCompareSession.tell(ADBLocalCompareAttributesSession.CompareJoinAttributes
                .builder()
                .terms(this.groupedQueryTerms.get(command.sourceAttribute).toArray(new ADBJoinQueryTerm[0]))
                .sourceAttributeName(command.sourceAttribute)
                .sourceAttributes(command.sourceAttributes)
                .build());
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinAttributesCompared(JoinAttributesComparedFor command) {
        this.getContext().getLog().info("Received " + command.joinCandidates.size() + " join candidates to be " +
                "intersected");
        if (this.joinCandidates.isEmpty()) {
            this.joinCandidates.addAll(command.joinCandidates);
        } else {
            this.joinCandidates.retainAll(command.joinCandidates);
        }
        this.groupedQueryTerms.remove(command.sourceAttributeName);
        this.getContext().getLog().info("Remaining attributes to be joined: " + this.groupedQueryTerms.keySet());
        return this.submitResults();
    }

    private Behavior<Command> submitResults() {
        if (this.groupedQueryTerms.size() > 0) {
            return Behaviors.same();
        }
        this.getContext().getLog().info("About to return " + this.joinCandidates.size() + " join candidates to " + this.session);
        ADBJoinWithShardSession.HandleJoinShardsResults message = new ADBJoinWithShardSession.HandleJoinShardsResults(
                this.joinCandidates
                        .stream()
                        .map(pair -> new ADBPair<>(pair.getKey(), this.data.get(pair.getValue())))
                        .collect(Collectors.toSet()));

        this.getContext().spawn(ADBLargeMessageSenderFactory.createDefault(message, this.largeMessageSenderWrapping,
                "JoinAttributeResults"),
                ADBLargeMessageSenderFactory.senderName(this.getContext().getSelf(), this.session, message.getClass()
                        , "JoinAttributeResults"))
            .tell(new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(this.session), message.getClass()));
        return Behaviors.same();
    }

    @Override
    protected Behavior<Command> handleLargeMessageTransferCompleted(ADBLargeMessageSender.TransferCompleted response) {
        this.getContext().getLog().info("Results have been submitted - Terminating");
        return Behaviors.stopped();
    }
}