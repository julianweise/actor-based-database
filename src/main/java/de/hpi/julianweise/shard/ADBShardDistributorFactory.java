package de.hpi.julianweise.shard;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;

public class ADBShardDistributorFactory {
    public static Behavior<ADBShardDistributor.Command> createDefault() {
        return Behaviors.setup(context -> {
                    ActorRef<Receptionist.Listing> wrapper = ADBShardDistributorFactory.createListingWrapper(context);
                    context.getSystem().receptionist().tell(Receptionist.subscribe(ADBShard.SERVICE_KEY, wrapper));
                    return new ADBShardDistributor(context);
                });
    }

    private static ActorRef<Receptionist.Listing> createListingWrapper(ActorContext<ADBShardDistributor.Command> context) {
        return context.messageAdapter(Receptionist.Listing.class, ADBShardDistributor.WrappedListing::new);
    }
}
