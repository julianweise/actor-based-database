package de.hpi.julianweise.utility.largemessage;

import akka.actor.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.ReceiveBuilder;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import lombok.AllArgsConstructor;
import lombok.val;

import java.util.Set;

public abstract class ADBLargeMessageActor extends AbstractBehavior<ADBLargeMessageActor.Command> {

    public static void sendMessage(ActorContext<?> sender, ActorRef receiver,
                                   akka.actor.typed.ActorRef<ADBLargeMessageSender.Response> responseReceiver,
                                   ADBLargeMessageSender.LargeMessage msg) {
        val senderClassic = sender.classicActorContext().self();
        String receiverName = ADBLargeMessageSenderFactory.name(senderClassic, receiver, msg.getClass());
        val sendActor = sender.spawn(ADBLargeMessageSenderFactory.createDefault(msg, responseReceiver), receiverName);
        sendActor.tell(new ADBLargeMessageSender.StartTransfer(receiver, msg.getClass()));
    }

    protected final Set<akka.actor.typed.ActorRef<ADBLargeMessageReceiver.Command>> openReceiverSessions;

    public ADBLargeMessageActor(ActorContext<Command> context) {
        super(context);
        this.openReceiverSessions = new ObjectOpenHashSet<>();
    }

    public interface Command {}

    @AllArgsConstructor
    private static class ReceiverTerminated implements Command {
        akka.actor.typed.ActorRef<ADBLargeMessageReceiver.Command> receiver;
    }


    @Override
    public ReceiveBuilder<Command> newReceiveBuilder() {
        return super.newReceiveBuilder()
                    .onMessage(ReceiverTerminated.class, this::handleReceiverTerminated)
                    .onMessage(ADBLargeMessageReceiver.InitializeTransfer.class, this::handleStartLargeMessageTransfer);
    }

    protected Behavior<Command> handleStartLargeMessageTransfer(ADBLargeMessageReceiver.InitializeTransfer command) {
        val name = ADBLargeMessageReceiverFactory.receiverName(this.getContext().getSelf(), command.getType());
        val receiver = getContext().spawn(ADBLargeMessageReceiverFactory.createDefault(getContext().classicActorContext().self()),name);
        receiver.tell(command);
        this.openReceiverSessions.add(receiver);
        this.getContext().watchWith(receiver, new ReceiverTerminated(receiver));
        return Behaviors.same();
    }

    protected Behavior<Command> handleReceiverTerminated(ReceiverTerminated command) {
        this.openReceiverSessions.remove(command.receiver);
        this.handleSenderTerminated();
        return Behaviors.same();
    }

    protected abstract void handleSenderTerminated();
}
