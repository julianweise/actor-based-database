package de.hpi.julianweise.master;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

public class ADBMasterSupervisorFactory {

    public static Behavior<ADBMasterSupervisor.Command> createDefault(MasterConfiguration masterConfiguration,
                                                                      Behavior<ADBLoadAndDistributeDataProcess.Command> loadAndDistributeProcess) {
        return Behaviors.setup(context -> new ADBMasterSupervisor(context, masterConfiguration,
                loadAndDistributeProcess));
    }
}
