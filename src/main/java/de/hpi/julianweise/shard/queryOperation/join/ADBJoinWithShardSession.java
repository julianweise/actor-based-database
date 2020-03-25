package de.hpi.julianweise.shard.queryOperation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBLargeMessageSender;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBLargeMessageSenderFactory;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Set;

public class ADBJoinWithShardSession extends ADBLargeMessageActor {

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class RegisterHandler implements Command, CborSerializable {
        private ActorRef<ADBJoinWithShardSessionHandler.Command> sessionHandler;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class HandleJoinShardsResults implements Command, ADBJoinQuerySessionHandler.Command, ADBLargeMessageSender.LargeMessage {
        private Set<ADBPair<Integer, ADBEntityType>> joinCandidates;
    }

    private ActorRef<ADBJoinWithShardSessionHandler.Command> sessionHandler;
    private final ActorRef<ADBJoinQuerySessionHandler.Command> supervisor;
    private final ADBQuery query;
    private final Map<String, ADBSortedEntityAttributes> sortedJoinAttributes;


    public ADBJoinWithShardSession(ActorContext<Command> context, ADBQuery query,
                                   Map<String, ADBSortedEntityAttributes> sortedJoinAttributes,
                                   ActorRef<ADBJoinQuerySessionHandler.Command> supervisor) {
        super(context);
        this.query = query;
        this.supervisor = supervisor;
        this.sortedJoinAttributes = sortedJoinAttributes;
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
        this.getContext().getLog().info("Start new Session for joining with " + this.sessionHandler.path().name());
        this.sendRequiredJoinAttributes();
        return Behaviors.same();
    }

    private void sendRequiredJoinAttributes() {
        this.query.getTerms().stream()
                  .map(term -> ((ADBJoinQueryTerm) term).getSourceAttributeName())
                  .distinct()
                  .forEach(this::sendRequiredJoinAttribute);
    }

    private void sendRequiredJoinAttribute(String attributeName) {
        assert this.sessionHandler != null;

        ADBJoinWithShardSessionHandler.CompareJoinAttributesFor message = ADBJoinWithShardSessionHandler
                .CompareJoinAttributesFor
                .builder()
                .sourceAttribute(attributeName)
                .sourceAttributes(this.sortedJoinAttributes.get(attributeName).getAllWithOriginalIndex())
                .build();

        this.getContext().spawn(ADBLargeMessageSenderFactory.createDefault(message, this.largeMessageSenderWrapping,
                attributeName),
                ADBLargeMessageSenderFactory.senderName(this.getContext().getSelf(), this.sessionHandler,
                        message.getClass(), attributeName))
            .tell(new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(this.sessionHandler), message.getClass()));

    }

    private Behavior<Command> handleJoinShardsResults(HandleJoinShardsResults command) {
        this.supervisor.tell(command);
        return Behaviors.stopped();
    }

    @Override
    protected Behavior<Command> handleLargeMessageTransferCompleted(ADBLargeMessageSender.TransferCompleted response) {
        this.getContext().getLog().info("Join attributes have been transferred.");
        return Behaviors.same();
    }

}
