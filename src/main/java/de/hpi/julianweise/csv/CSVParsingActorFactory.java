package de.hpi.julianweise.csv;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

public class CSVParsingActorFactory {

    public static Behavior<CSVParsingActor.Command> createForFile(String filePath) {
        return Behaviors.setup(context -> new CSVParsingActor(context, filePath));
    }
}
