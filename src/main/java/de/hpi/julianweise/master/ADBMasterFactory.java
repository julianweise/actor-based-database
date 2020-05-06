package de.hpi.julianweise.master;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.master.data_loading.ADBLoadAndDistributeDataProcess;

public class ADBMasterFactory {

    public static Behavior<ADBMaster.Command> createDefault(Behavior<ADBLoadAndDistributeDataProcess.Command>
                                                                              loadAndDistributeProcess) {
        return Behaviors.setup(context -> new ADBMaster(context, loadAndDistributeProcess));
    }
}
