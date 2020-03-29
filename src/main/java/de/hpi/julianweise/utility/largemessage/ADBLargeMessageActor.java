package de.hpi.julianweise.utility.largemessage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.ReceiveBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;

public abstract class ADBLargeMessageActor extends AbstractBehavior<ADBLargeMessageActor.Command> {

    protected final ActorRef<ADBLargeMessageSender.Response> largeMessageSenderWrapping;

    public interface Command {}
    @AllArgsConstructor
    @Getter
    public static class WrappedLargeMessageSenderResponse implements Command {
        private ADBLargeMessageSender.Response response;
    }

    public ADBLargeMessageActor(ActorContext<Command> context) {
        super(context);
        this.largeMessageSenderWrapping = context.messageAdapter(ADBLargeMessageSender.Response.class,
                WrappedLargeMessageSenderResponse::new);
    }

    protected ReceiveBuilder<Command> createReceiveBuilder() {
        return newReceiveBuilder()
                .onMessage(ADBLargeMessageReceiver.InitializeTransfer.class, this::handleStartLargeMessageTransfer)
                .onMessage(WrappedLargeMessageSenderResponse.class, this::handleWrappedLargeMessageSenderResponse);
    }

    protected Behavior<Command> handleStartLargeMessageTransfer(ADBLargeMessageReceiver.InitializeTransfer command) {
        this.getContext().spawn(ADBLargeMessageReceiverFactory.createDefault(this.getContext().classicActorContext().self(),
                command.getType(), command.getRespondTo()),
                ADBLargeMessageReceiverFactory.receiverName(this.getContext().getSelf(), command.getType()))
            .tell(command);
        return Behaviors.same();
    }

    private Behavior<Command> handleWrappedLargeMessageSenderResponse(WrappedLargeMessageSenderResponse response) {
        if (response.getResponse() instanceof ADBLargeMessageSender.TransferCompleted) {
            return this.handleLargeMessageTransferCompleted((ADBLargeMessageSender.TransferCompleted) response.getResponse());
        }
        this.getContext().getLog().warn(String.format("[%s] Received LargeMessageSender response of unknown subtype."
                , this.getClass().getName().split(".")[this.getClass().getName().split(".").length - 1]));
        return Behaviors.same();
    }

    protected abstract Behavior<Command> handleLargeMessageTransferCompleted(ADBLargeMessageSender.TransferCompleted response);
}
