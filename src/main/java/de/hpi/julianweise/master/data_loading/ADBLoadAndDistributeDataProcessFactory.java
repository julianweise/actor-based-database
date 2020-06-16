package de.hpi.julianweise.master.data_loading;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.csv.CSVParsingActor;

public class ADBLoadAndDistributeDataProcessFactory {

    public static Behavior<ADBLoadAndDistributeDataProcess.Command> createDefault(Behavior<CSVParsingActor.Command> csvParser) {
        return Behaviors.setup(context -> new ADBLoadAndDistributeDataProcess(context, csvParser));
    }
}
