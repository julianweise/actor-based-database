package de.hpi.julianweise.csv;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.ADBEntityFactory;

public class CSVParsingActorFactory {

    public static Behavior<CSVParsingActor.Command> createForFile(String filePath, ADBEntityFactory domainFactory) {
        return Behaviors.setup(context -> new CSVParsingActor(context, filePath, domainFactory));
    }
}
