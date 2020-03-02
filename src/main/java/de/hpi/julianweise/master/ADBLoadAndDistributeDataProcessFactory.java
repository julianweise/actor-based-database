package de.hpi.julianweise.master;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.shard.ADBShardDistributor;

public class ADBLoadAndDistributeDataProcessFactory {

    public static Behavior<ADBLoadAndDistributeDataProcess.Command> createDefault(Behavior<CSVParsingActor.Command> csvParser,
                                                                                  Behavior<ADBShardDistributor.Command> shardDistributor) {
        return Behaviors.setup(context -> new ADBLoadAndDistributeDataProcess(context, csvParser, shardDistributor));
    }
}
