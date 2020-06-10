package de.hpi.julianweise.utility.largemessage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

import java.util.UUID;

public class ADBLargeMessageSenderFactory {

    public static Behavior<ADBLargeMessageSender.Command> createDefault(ADBLargeMessageSender.LargeMessage message,
                                                                        ActorRef<ADBLargeMessageSender.Response> respondTo) {
        return Behaviors.setup(context -> new ADBLargeMessageSender(context, message, respondTo));
    }

    public static String name(akka.actor.ActorRef sender,
                              akka.actor.ActorRef receiver,
                              Class<? extends ADBLargeMessageSender.LargeMessage> payLoadMessage) {
        return "ADBLargeMessageSender@" + sender.path().name() + "-::" + payLoadMessage.getSimpleName() + ":" +
                "::-" + receiver.path().name() + "-" + UUID.randomUUID().toString();
    }
}
