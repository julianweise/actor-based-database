package de.hpi.julianweise.slave.query;

import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Routers;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;

import static de.hpi.julianweise.settings.Settings.SettingsProvider;

public class ADBQueryManagerFactory {

    public static void createSingleton(ActorContext<?> context) {
        if (ADBQueryManager.getInstance() == null) {
            SettingsImpl settings = SettingsProvider.get(context.getSystem());

            ADBQueryManager.setInstance(context.spawn(Behaviors.setup(ADBQueryManager::new), "ADBQueryManager"));
            ADBQueryManager.setWorkerPool(context.spawn(Routers.pool(settings.NUMBER_OF_THREADS, Behaviors
                    .supervise(GenericWorker.create()).onFailure(SupervisorStrategy.restart())), "worker-pool"));
        }
    }
}
