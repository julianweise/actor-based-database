package de.hpi.julianweise.master.data_loading;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;

public class ADBLoadAndDistributeDataProcessFactory {

    public static Behavior<ADBLoadAndDistributeDataProcess.Command> createDefault(Behavior<CSVParsingActor.Command> csvParser) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.subscribe(ADBPartitionManager.SERVICE_KEY,
                    ADBLoadAndDistributeDataProcessFactory.createReceptionistAdapter(context)));
            return new ADBLoadAndDistributeDataProcess(context, csvParser);
        });
    }

    private static ActorRef<Receptionist.Listing> createReceptionistAdapter(ActorContext<ADBLoadAndDistributeDataProcess.Command> ctx) {
        return ctx.messageAdapter(Receptionist.Listing.class,
                ADBLoadAndDistributeDataProcess.WrappedReceptionistListing::new);
    }
}
