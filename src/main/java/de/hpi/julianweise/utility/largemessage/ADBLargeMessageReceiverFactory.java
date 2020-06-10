package de.hpi.julianweise.utility.largemessage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

import java.util.UUID;

public class ADBLargeMessageReceiverFactory {

    public static Behavior<ADBLargeMessageReceiver.Command> createDefault(akka.actor.ActorRef originalReceiver) {
        return Behaviors.setup(context -> new ADBLargeMessageReceiver(context, originalReceiver));
    }

    public static String receiverName(ActorRef<?> receiver, Class<? extends ADBLargeMessageSender.LargeMessage> payLoad) {
        return  "Unknown-::" + payLoad.getSimpleName() + "::-" + receiver.path().name() + "@ADBLargeMessageReceiver" +
                "-" + UUID.randomUUID().toString();
    }

}
