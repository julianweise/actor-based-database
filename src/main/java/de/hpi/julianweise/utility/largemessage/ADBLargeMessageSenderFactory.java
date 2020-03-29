package de.hpi.julianweise.utility.largemessage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

public class ADBLargeMessageSenderFactory {

    public static Behavior<ADBLargeMessageSender.Command> createDefault(ADBLargeMessageSender.LargeMessage message,
                                                                        ActorRef<ADBLargeMessageSender.Response> respondTo) {
        return Behaviors.setup(context -> new ADBLargeMessageSender(context, message, respondTo));
    }

    public static String senderName(ActorRef<?> sender,
                                    ActorRef<?> receiver,
                                    Class<? extends ADBLargeMessageSender.LargeMessage> payLoadMessage,
                                    String payload) {
        return "ADBLargeMessageSender@" + sender.path().name() + "-::" + payLoadMessage.getSimpleName() + ":" + payload +
                "::-" + receiver.path().name();
    }
}
