package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.benchmarking.ADBQueryPerformanceSampler;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSenderFactory;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

public class ADBJoinWithShardSession extends ADBLargeMessageActor {

    private ActorRef<ADBJoinWithShardSessionHandler.Command> sessionHandler;
    private final ActorRef<ADBJoinQuerySessionHandler.Command> supervisor;
    private final ADBJoinQuery query;
    private final Map<String, ADBSortedEntityAttributes> sortedJoinAttributes;
    private final int localShardId;
    private final int remoteShardId;

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class RegisterHandler implements Command, CborSerializable {
        private ActorRef<ADBJoinWithShardSessionHandler.Command> sessionHandler;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class HandleJoinShardsResults implements ADBLargeMessageSender.LargeMessage, Command {
        private List<ADBPair<Integer, ADBEntity>> joinCandidates;
    }


    public ADBJoinWithShardSession(ActorContext<Command> context, ADBJoinQuery query,
                                   Map<String, ADBSortedEntityAttributes> sortedJoinAttributes,
                                   ActorRef<ADBJoinQuerySessionHandler.Command> supervisor, int localShardId,
                                   int remoteShardId) {
        super(context);
        ADBQueryPerformanceSampler.log(true, this.getClass().getSimpleName(), "Join with Shard #" + remoteShardId);
        this.query = query;
        this.supervisor = supervisor;
        this.sortedJoinAttributes = sortedJoinAttributes;
        this.localShardId = localShardId;
        this.remoteShardId = remoteShardId;
    }

    @Override
    public Receive<Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(RegisterHandler.class, this::handleRegisterHandler)
                   .onMessage(HandleJoinShardsResults.class, this::handleJoinShardsResults)
                   .build();
    }

    private Behavior<Command> handleRegisterHandler(RegisterHandler command) {
        this.sessionHandler = command.sessionHandler;
        this.getContext().getLog().info("Created new session on shard #" + this.localShardId + " (local) to join with" +
                " shard #" + this.remoteShardId + " (remote)");
        this.query.getAllFields().forEach(this::sendRequiredJoinAttribute);
        return Behaviors.same();
    }

    private void sendRequiredJoinAttribute(String attributeName) {
        ADBJoinWithShardSessionHandler.ForeignAttributes message = ADBJoinWithShardSessionHandler.ForeignAttributes
                .builder()
                .attributeName(attributeName)
                .attributeValues(this.sortedJoinAttributes.get(attributeName).getAllWithOriginalIndex())
                .build();

        this.getContext().spawn(ADBLargeMessageSenderFactory.createDefault(message, this.largeMessageSenderWrapping),
                ADBLargeMessageSenderFactory.senderName(this.getContext().getSelf(), this.sessionHandler,
                        message.getClass(), attributeName))
            .tell(new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(this.sessionHandler), message.getClass()));

    }

    private Behavior<Command> handleJoinShardsResults(HandleJoinShardsResults command) {
        this.supervisor.tell(new ADBJoinQuerySessionHandler.HandleJoinShardResults(command.getJoinCandidates()));
        ADBQueryPerformanceSampler.log(false, this.getClass().getSimpleName(),
                "Join with Shard #" + this.remoteShardId);
        return Behaviors.stopped();
    }

    @Override
    protected Behavior<Command> handleLargeMessageTransferCompleted(ADBLargeMessageSender.TransferCompleted response) {
        this.getContext().getLog().info("Join attributes have been transferred.");
        return Behaviors.same();
    }

}
