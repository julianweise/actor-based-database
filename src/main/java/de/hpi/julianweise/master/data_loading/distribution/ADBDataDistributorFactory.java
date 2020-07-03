package de.hpi.julianweise.master.data_loading.distribution;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;

public class ADBDataDistributorFactory {

    public static Behavior<ADBDataDistributor.Command> createDefault(ActorRef<ADBLargeMessageActor.Command> manager) {
        return Behaviors.setup(context -> new ADBDataDistributor(context, manager));
    }
}
