package de.hpi.julianweise.utility.largemessage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

public class ADBLargeMessageSenderFactory {

    public static Behavior<ADBLargeMessageSender.Command> createDefault(Object message,
                                                                        akka.actor.typed.ActorRef<ADBLargeMessageSender.Response> respondTo,
                                                                        String sessionName) {
        return Behaviors.setup(context -> new ADBLargeMessageSender(context, message, respondTo, sessionName));
    }

    public static String senderName(ActorRef<ADBLargeMessageActor.Command> sender,
                                    ActorRef<ADBLargeMessageActor.Command> receiver,
                                    Class<? extends ADBLargeMessageSender.LargeMessage> payLoad,
                                    String transferSessionName) {
        return "ADBLargeMessageSender@" + sender.path().name() + "-::" + payLoad.getSimpleName() + ":" + transferSessionName + "::-" + receiver.path().name();
    }
}
