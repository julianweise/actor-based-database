package de.hpi.julianweise.utility.largemessage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.KryoSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.NotSerializableException;

public class ADBLargeMessageReceiver<T> extends AbstractBehavior<ADBLargeMessageReceiver.Command> {

    public interface Command extends ADBLargeMessageActor.Command {
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class InitializeTransfer implements Command, CborSerializable {
        private String originalSender;
        private ActorRef<ADBLargeMessageSender.Command> respondTo;
        private int totalSize;
        private Class<? extends ADBLargeMessageSender.LargeMessage> type;
        private String transferSessionName;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class ReceiveChunk implements Command, KryoSerializable {
        private byte[] chunk;
        private boolean lastChunk;
    }

    private byte[] payload;
    private int payloadPointer = 0;
    private final ActorRef<T> originalReceiver;
    private ActorRef<ADBLargeMessageSender.Command> sender;
    private boolean hasStarted;
    private final Serialization serialization;
    private final Class<? extends CborSerializable> messageType;

    public ADBLargeMessageReceiver(ActorContext<Command> context, ActorRef<T> originalReceiver,
                                   Class<? extends CborSerializable> messageType) {
        super(context);
        this.serialization = SerializationExtension.get(this.getContext().getSystem());
        this.originalReceiver = originalReceiver;
        this.messageType = messageType;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitializeTransfer.class, this::handleInitializeTransfer)
                .onMessage(ReceiveChunk.class, this::handleReceiveNextChunk)
                .build();
    }

    private Behavior<Command> handleInitializeTransfer(InitializeTransfer command) {
        if (this.hasStarted) {
            this.getContext().getLog().warn(String.format("%s attempted to initialize data " +
                    "transfer for %s again that has already been initialized by %s", command.respondTo, command.type,
                    this.sender));
            return Behaviors.same();
        }
        this.hasStarted = true;
        this.payload = new byte[command.totalSize];
        this.sender = command.respondTo;
        this.sender.tell(new ADBLargeMessageSender.SendNextChunk(this.getContext().getSelf()));
        return Behaviors.same();
    }

    private Behavior<Command> handleReceiveNextChunk(ReceiveChunk command) throws NotSerializableException {
        System.arraycopy(command.chunk, 0, this.payload, this.payloadPointer, command.chunk.length);
        this.payloadPointer += command.chunk.length;
        return this.concludeTransfer(command);
    }

    @SuppressWarnings("unchecked")
    private Behavior<Command> concludeTransfer(ReceiveChunk command) throws NotSerializableException {
        if (!command.lastChunk) {
            this.sender.tell(new ADBLargeMessageSender.SendNextChunk(this.getContext().getSelf()));
            return Behaviors.same();
        }
        this.getContext().getLog().info("Received all data - Terminating");
        Serializer serializer = serialization.serializerFor(this.messageType);
        Object message = serializer.fromBinary(this.payload, this.messageType);
        this.originalReceiver.tell((T) this.messageType.cast(message));
        return Behaviors.stopped();
    }
}
