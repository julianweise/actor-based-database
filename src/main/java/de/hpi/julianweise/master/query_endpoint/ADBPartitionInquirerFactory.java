package de.hpi.julianweise.master.query_endpoint;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.slave.query.ADBQueryManager;

public class ADBPartitionInquirerFactory {

    public static Behavior<ADBPartitionInquirer.Command> createDefault() {
        return Behaviors.setup(context -> {
            ActorRef<Receptionist.Listing> wrapper = ADBPartitionInquirerFactory.createListingWrapper(context);
            context.getSystem().receptionist().tell(Receptionist.subscribe(ADBQueryManager.SERVICE_KEY, wrapper));
            return new ADBPartitionInquirer(context);
        });
    }

    private static ActorRef<Receptionist.Listing> createListingWrapper(ActorContext<ADBPartitionInquirer.Command> context) {
        return context.messageAdapter(Receptionist.Listing.class, ADBPartitionInquirer.WrappedListing::new);
    }


}
