package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSenderFactory;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ADBJoinWithShardSessionHandler extends ADBLargeMessageActor {

    private final ActorRef<ADBJoinWithShardSession.Command> session;
    private final List<ADBEntityType> data;
    private final ActorRef<ADBJoinQueryComparator.Command> joinQueryComparator;
    private ActorRef<ADBJoinQueryComparator.Command> joinInverseQueryComparator;
    private List<ADBPair<Integer, ADBEntityType>> joinCandidates = null;

    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class ForeignAttributes implements Command, ADBLargeMessageSender.LargeMessage {
        private String attributeName;
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
        @JsonSubTypes({
                              @JsonSubTypes.Type(value = String.class, name = "String"),
                              @JsonSubTypes.Type(value = Integer.class, name = "Integer"),
                              @JsonSubTypes.Type(value = Float.class, name = "Float"),
                              @JsonSubTypes.Type(value = Double.class, name = "Double"),
                              @JsonSubTypes.Type(value = Character.class, name = "Character"),
                              @JsonSubTypes.Type(value = Boolean.class, name = "Boolean"),
                      })
        private List<ADBPair<Comparable<?>, Integer>> attributeValues;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class ForeignAttributesCompared implements Command {
        private List<ADBKeyPair> joinCandidates;
        private ActorRef<ADBJoinQueryComparator.Command> sender;
    }

    public ADBJoinWithShardSessionHandler(ActorContext<Command> context,
                                          ActorRef<ADBJoinWithShardSession.Command> session,
                                          ADBQuery query,
                                          Map<String, ADBSortedEntityAttributes> localSortedAttributes,
                                          List<ADBEntityType> data, int localShardId, int remoteShardId) {
        super(context);
        session.tell(new ADBJoinWithShardSession.RegisterHandler(this.getContext().getSelf()));

        ADBJoinQuery joinQuery = (ADBJoinQuery) query;
        this.session = session;
        this.data = data;
        this.joinQueryComparator = this.spawnJoinQueryComparator(joinQuery, localSortedAttributes, "QueryComparator");
        if (!this.isSelfComparison()) {
            this.joinInverseQueryComparator = this.spawnJoinQueryComparator(joinQuery.getReverse(), localSortedAttributes, "QueryComparatorInverse");
        }

        this.getContext().getLog().info("Create new session handler on  shard #" + localShardId + " (local) to " +
                "join with shard #" + remoteShardId + " (remote).");
    }

    private ActorRef<ADBJoinQueryComparator.Command> spawnJoinQueryComparator(ADBJoinQuery query,
                                                                              Map<String, ADBSortedEntityAttributes> localSortedAttributes,
                                                                              String name) {
        return this.getContext().spawn(ADBJoinQueryComparatorFactory.createDefault(query, localSortedAttributes,
                this.getContext().getSelf()), name);
    }

    private boolean isSelfComparison() {
        return this.session.path().root().equals(this.getContext().getSelf().path().root());
    }

    @Override
    public Receive<Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(ForeignAttributes.class, this::handleCompareJoinAttributes)
                   .onMessage(ForeignAttributesCompared.class, this::handleForeignAttributesCompared)
                   .build();
    }


    private Behavior<Command> handleCompareJoinAttributes(ForeignAttributes command) {
        this.getContext().getLog().info("Received attributes for join comparison: " + command.attributeName);

        this.joinQueryComparator.tell(new ADBJoinQueryComparator.ProcessForeignAttributes(
                command.attributeName,
                command.attributeValues));

        if (this.joinInverseQueryComparator == null) {
            return Behaviors.same();
        }

        this.joinInverseQueryComparator.tell(new ADBJoinQueryComparator.ProcessForeignAttributes(
                command.attributeName,
                command.attributeValues));

        return Behaviors.same();
    }

    private Behavior<Command> handleForeignAttributesCompared(ForeignAttributesCompared result) {
        if (this.isSelfComparison()) {
            this.joinCandidates = this.materializeResult(result);
            this.submitResults();
            return Behaviors.same();
        }
        if (this.joinCandidates == null) {
            this.joinCandidates = this.materializeResult(result);
            return Behaviors.same();
        }
        this.joinCandidates.addAll(this.materializeResult(result));
        this.submitResults();
        return Behaviors.same();
    }

    private List<ADBPair<Integer, ADBEntityType>> materializeResult(ForeignAttributesCompared result) {
        boolean isTupleFlipped = this.joinInverseQueryComparator != null
                        && result.sender.path().equals(this.joinInverseQueryComparator.path());
        ArrayList<ADBPair<Integer, ADBEntityType>> semiMaterializedResults = new ArrayList<>(data.size());
        for (ADBKeyPair tuple : result.joinCandidates) {
            semiMaterializedResults.add(new ADBPair<>(isTupleFlipped, tuple.getKey(), this.data.get(tuple.getValue())));
        }
        return semiMaterializedResults;
    }

    private void submitResults() {
        this.getContext().getLog().info("About to return " + this.joinCandidates.size() + " join candidates to " + this.session);
        ADBJoinWithShardSession.HandleJoinShardsResults message =
                new ADBJoinWithShardSession.HandleJoinShardsResults(this.joinCandidates);
        this.getContext().spawn(ADBLargeMessageSenderFactory.createDefault(message, this.largeMessageSenderWrapping),
                ADBLargeMessageSenderFactory.senderName(this.getContext().getSelf(), this.session, message.getClass(),
                        this.joinCandidates.size() + "-candidates-to-" + this.largeMessageSenderWrapping.hashCode()))
            .tell(new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(this.session), message.getClass()));
    }

    @Override
    protected Behavior<Command> handleLargeMessageTransferCompleted(ADBLargeMessageSender.TransferCompleted response) {
        this.getContext().getLog().info("Results have been submitted - Terminating");
        return Behaviors.stopped();
    }
}
