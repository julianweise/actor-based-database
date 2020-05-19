package de.hpi.julianweise.utility.largemessage;

import akka.actor.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Settings;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.NotSerializableException;
import java.util.Arrays;


public class ADBLargeMessageSender extends AbstractBehavior<ADBLargeMessageSender.Command> {

    private final Serialization serialization;
    private int dataSent = 0;
    private final byte[] payload;
    private final int chunkSize;
    private final akka.actor.typed.ActorRef<ADBLargeMessageSender.Response> supervisor;

    public interface LargeMessage extends KryoSerializable, ADBLargeMessageActor.Command {
    }

    public interface Command extends CborSerializable {
    }

    public interface Response extends CborSerializable {
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class StartTransfer implements Command {
        private ActorRef receiver;
        private Class<? extends LargeMessage> type;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class SendNextChunk implements Command {
        private akka.actor.typed.ActorRef<ADBLargeMessageReceiver.Command> respondTo;

    }
    @NoArgsConstructor
    @Getter
    public static class TransferCompleted implements Response {

    }

    public static int getChunkSize(Settings settings) {
        long maxMessageSize = settings.config().getBytes("akka.remote.artery.advanced.maximum-frame-size");
        return Math.round(maxMessageSize * 0.6f);
    }

    public ADBLargeMessageSender(ActorContext<Command> context, LargeMessage serializableMessage,
                                 akka.actor.typed.ActorRef<ADBLargeMessageSender.Response> supervisor) throws NotSerializableException {
        super(context);
        this.serialization = SerializationExtension.get(this.getContext().getSystem());
        this.payload = this.serializePayload(serializableMessage);
        this.chunkSize = ADBLargeMessageSender.getChunkSize(context.getSystem().settings());
        this.supervisor = supervisor;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartTransfer.class, this::handleStartTransfer)
                .onMessage(SendNextChunk.class, this::handleSendNextChunk)
                .build();
    }

    private Behavior<Command> handleStartTransfer(StartTransfer command) {
        command.getReceiver().tell(new ADBLargeMessageReceiver.InitializeTransfer(
                        this.getContext().getSelf(), this.payload.length, command.getType()),
                this.getContext().classicActorContext().self());
        return Behaviors.same();
    }

    private Behavior<Command> handleSendNextChunk(SendNextChunk command) {
        int end = Math.min(payload.length, this.dataSent + this.chunkSize);
        command.getRespondTo().tell(new ADBLargeMessageReceiver.ReceiveChunk(
                Arrays.copyOfRange(this.payload, this.dataSent, end),
                end >= payload.length
        ));
        this.dataSent = end;
        if (end >= payload.length) {
            return this.killSender();
        }
        return Behaviors.same();
    }

    private Behavior<Command> killSender() {
        if (this.supervisor != null) {
            this.supervisor.tell(new TransferCompleted());
        }
        return Behaviors.stopped();
    }

    private byte[] serializePayload(LargeMessage payload) throws NotSerializableException {
        Serializer serializer = serialization.serializerFor(payload.getClass());
        return serializer.toBinary(payload);
    }
}