package de.hpi.julianweise.utility.largemessage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

import java.util.UUID;

public class ADBLargeMessageReceiverFactory {

    public static Behavior<ADBLargeMessageReceiver.Command> createDefault(
            ActorRef<ADBLargeMessageActor.Command> originalReceiver,
            Class<? extends ADBLargeMessageSender.LargeMessage> messageType,
            ActorRef<ADBLargeMessageSender.Command> sender) {
        return Behaviors.setup(context -> new ADBLargeMessageReceiver(context, originalReceiver, messageType, sender));
    }

    public static String receiverName(ActorRef<ADBLargeMessageActor.Command> receiver,
                                    Class<? extends ADBLargeMessageSender.LargeMessage> payLoad) {
        return  "Unknown-::" + payLoad.getSimpleName() + "::-" + receiver.path().name() + "@ADBLargeMessageReceiver" +
                "-" + UUID.randomUUID().toString();
    }

}
