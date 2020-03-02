package de.hpi.julianweise.shard;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

public class ADBShardFactory {

    public static Behavior<ADBShard.Command> createDefault() {
        return Behaviors.setup(ADBShard::new);
    }
}
