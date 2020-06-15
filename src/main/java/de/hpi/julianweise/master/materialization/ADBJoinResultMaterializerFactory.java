package de.hpi.julianweise.master.materialization;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ADBJoinResultMaterializerFactory {

    public static Behavior<ADBJoinResultMaterializer.Command> createDefault(
            ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers,
            ActorRef<ADBJoinResultMaterializer.Response> supervisor) {
        return Behaviors.setup(context -> new ADBJoinResultMaterializer(context, partitionManagers, supervisor));
    }
}
