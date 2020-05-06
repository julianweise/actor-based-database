package de.hpi.julianweise.master.data_loading.distribution;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;

public class ADBDataDistributorFactory {
    public static Behavior<ADBDataDistributor.Command> createDefault() {
        return Behaviors.setup(context -> {
                    ActorRef<Receptionist.Listing> wrapper = ADBDataDistributorFactory.createListingWrapper(context);
                    context.getSystem().receptionist().tell(Receptionist.subscribe(ADBPartitionManager.SERVICE_KEY, wrapper));
                    return new ADBDataDistributor(context);
                });
    }

    private static ActorRef<Receptionist.Listing> createListingWrapper(ActorContext<ADBDataDistributor.Command> context) {
        return context.messageAdapter(Receptionist.Listing.class, ADBDataDistributor.WrappedListing::new);
    }
}
