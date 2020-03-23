package de.hpi.julianweise.utility.largeMessageTransfer;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

public class ADBLargeMessageReceiverFactory {

    public static <T>Behavior<ADBLargeMessageReceiver.Command> createDefault(
            ActorRef<T> originalReceiver,
            Class<? extends ADBLargeMessageSender.LargeMessage> messageType) {
        return Behaviors.setup(context -> new ADBLargeMessageReceiver<>(context, originalReceiver, messageType));
    }

    public static String receiverName(String sender,
                                    ActorRef<ADBLargeMessageActor.Command> receiver,
                                    Class<? extends ADBLargeMessageSender.LargeMessage> payLoad,
                                      String transferSessionName) {
        return sender + "-::" + payLoad.getSimpleName() + ":" + transferSessionName + "::-" + receiver.path().name() + "@ADBLargeMessageReceiver";
    }

}
