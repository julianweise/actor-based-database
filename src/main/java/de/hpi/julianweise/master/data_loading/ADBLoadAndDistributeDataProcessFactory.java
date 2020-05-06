package de.hpi.julianweise.master.data_loading;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributor;

public class ADBLoadAndDistributeDataProcessFactory {

    public static Behavior<ADBLoadAndDistributeDataProcess.Command> createDefault(Behavior<CSVParsingActor.Command> csvParser,
                                                                                  Behavior<ADBDataDistributor.Command> shardDistributor) {
        return Behaviors.setup(context -> new ADBLoadAndDistributeDataProcess(context, csvParser, shardDistributor));
    }
}
