package de.hpi.julianweise.slave.partition;

import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;

public class ADBPartitionManagerFactory {

    public static void createSingleton(ActorContext<?> context) {
        if (ADBPartitionManager.getInstance() == null) {
            ADBPartitionManager.setInstance(context.spawn(Behaviors.setup(ADBPartitionManager::new), "ADBPartitionManager"));
        }
    }
}
