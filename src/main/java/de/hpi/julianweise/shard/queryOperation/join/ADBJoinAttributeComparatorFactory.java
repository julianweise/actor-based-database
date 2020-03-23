package de.hpi.julianweise.shard.queryOperation.join;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

public class ADBJoinAttributeComparatorFactory {

    public static Behavior<ADBJoinAttributeComparator.Command> createDefault() {
        return Behaviors.setup(ADBJoinAttributeComparator::new);
    }
}
