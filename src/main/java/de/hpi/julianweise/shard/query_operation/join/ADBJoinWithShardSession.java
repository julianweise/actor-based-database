package de.hpi.julianweise.shard.query_operation.join;

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
import de.hpi.julianweise.utility.KryoSerializable;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSenderFactory;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Set;

public class ADBJoinWithShardSession extends ADBLargeMessageActor {

    private ActorRef<ADBJoinWithShardSessionHandler.Command> sessionHandler;
    private final ActorRef<ADBJoinQuerySessionHandler.Command> supervisor;
    private final ADBQuery query;
    private final Map<String, ADBSortedEntityAttributes> sortedJoinAttributes;

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class RegisterHandler implements Command, CborSerializable {
        private ActorRef<ADBJoinWithShardSessionHandler.Command> sessionHandler;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class HandleJoinShardsResults implements ADBLargeMessageSender.LargeMessage, Command{
        private Set<ADBPair<Integer, ADBEntityType>> joinCandidates;
    }


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

        this.getContext().spawn(ADBLargeMessageSenderFactory.createDefault(message, this.largeMessageSenderWrapping),
                ADBLargeMessageSenderFactory.senderName(this.getContext().getSelf(), this.sessionHandler,
                        message.getClass(), attributeName))
            .tell(new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(this.sessionHandler), message.getClass()));

    }

    private Behavior<Command> handleJoinShardsResults(HandleJoinShardsResults command) {
        this.supervisor.tell(new ADBJoinQuerySessionHandler.HandleJoinShardResults(command.getJoinCandidates()));
        return Behaviors.stopped();
    }

    @Override
    protected Behavior<Command> handleLargeMessageTransferCompleted(ADBLargeMessageSender.TransferCompleted response) {
        this.getContext().getLog().info("Join attributes have been transferred.");
        return Behaviors.same();
    }

}
