package de.hpi.julianweise.utility.largemessage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;


public class ADBLargeMessageReceiver extends AbstractBehavior<ADBLargeMessageReceiver.Command> {

    private final akka.actor.ActorRef originalReceiver;
    private ActorRef<ADBLargeMessageSender.Command> sender;
    private Class<?> messageType;

    private final Serialization serialization;
    private byte[] payload;
    private int payloadPointer = 0;

    public interface Command {}

    public interface Response {}

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class InitializeTransfer implements Command, ADBLargeMessageActor.Command, CborSerializable {
        private ActorRef<ADBLargeMessageSender.Command> respondTo;
        private int totalSize;
        private Class<? extends ADBLargeMessageSender.LargeMessage> type;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class ReceiveChunk implements Command, KryoSerializable {
        private byte[] chunk;
        private boolean lastChunk;

    }

    public ADBLargeMessageReceiver(ActorContext<Command> context, akka.actor.ActorRef originalReceiver) {
        super(context);
        this.serialization = SerializationExtension.get(this.getContext().getSystem());
        this.originalReceiver = originalReceiver;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitializeTransfer.class, this::handleInitializeTransfer)
                .onMessage(ReceiveChunk.class, this::handleReceiveNextChunk)
                .build();
    }

    private Behavior<Command> handleInitializeTransfer(InitializeTransfer command) {
        if (this.payload != null) {
            this.getContext().getLog().warn(String.format("%s attempted to initialize transfer for %s again that has" +
                    "already been initialized by %s", command.respondTo, command.type, this.sender));
            return Behaviors.same();
        }
        this.messageType = command.getType();
        this.sender = command.getRespondTo();
        this.payload = new byte[command.totalSize];
        this.sender.tell(new ADBLargeMessageSender.SendNextChunk(this.getContext().getSelf()));
        return Behaviors.same();
    }

    private Behavior<Command> handleReceiveNextChunk(ReceiveChunk command) {
        System.arraycopy(command.getChunk(), 0, this.payload, this.payloadPointer, command.getChunk().length);
        this.payloadPointer += command.getChunk().length;
        return this.concludeTransfer(command);
    }

    private Behavior<Command> concludeTransfer(ReceiveChunk command) {
        if (!command.isLastChunk()) {
            this.sender.tell(new ADBLargeMessageSender.SendNextChunk(this.getContext().getSelf()));
            return Behaviors.same();
        }
        Object message = serialization.deserialize(this.payload, this.messageType).get();
        this.originalReceiver.tell(message, akka.actor.ActorRef.noSender());
        return Behaviors.stopped();
    }
}
