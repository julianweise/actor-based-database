package de.hpi.julianweise.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.shard.ADBShard;

public class ADBShardInquirerFactory {

    public static Behavior<ADBShardInquirer.Command> createDefault() {
        return Behaviors.setup(context -> {
            ActorRef<Receptionist.Listing> wrapper = ADBShardInquirerFactory.createListingWrapper(context);
            context.getSystem().receptionist().tell(Receptionist.subscribe(ADBShard.SERVICE_KEY, wrapper));
            return new ADBShardInquirer(context);
        });
    }

    private static ActorRef<Receptionist.Listing> createListingWrapper(ActorContext<ADBShardInquirer.Command> context) {
        return context.messageAdapter(Receptionist.Listing.class, ADBShardInquirer.WrappedListing::new);
    }


}
