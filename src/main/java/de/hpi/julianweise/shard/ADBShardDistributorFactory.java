package de.hpi.julianweise.shard;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.GroupRouter;
import akka.actor.typed.javadsl.Routers;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class ADBShardDistributorFactory {

    private static final int VIRTUAL_ROUTES_FACTOR_HASHING = 50;
    private static final Duration TIMER_DURATION = Duration.of(3000, ChronoUnit.MILLIS);

    public static Behavior<ADBShardDistributor.Command> createDefault() {
        GroupRouter<ADBShard.Command> clusterShards = Routers.group(ADBShard.SERVICE_KEY);
        clusterShards.withConsistentHashingRouting(VIRTUAL_ROUTES_FACTOR_HASHING, (ADBShardDistributorFactory::getHashingKey));

        return Behaviors.setup(context ->
                Behaviors.withTimers(timers -> {
                    timers.startTimerWithFixedDelay(new Object(), new ADBShardDistributor.CheckPendingDistributions(), TIMER_DURATION);
                    return new ADBShardDistributor(context, timers, clusterShards);
                }));
    }

    private static String getHashingKey(ADBShard.Command command) {
        ADBShard.PersistEntity castedCommand = (ADBShard.PersistEntity) command;
        return castedCommand.getEntity().getPrimaryKey().toString();
    }
}
