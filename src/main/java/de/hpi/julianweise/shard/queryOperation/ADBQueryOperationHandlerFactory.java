package de.hpi.julianweise.shard.queryOperation;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.queryOperation.ADBQueryOperationHandler.Command;

import java.util.Map;

public class ADBQueryOperationHandlerFactory {

    public static Behavior<Command> create(ADBShard.QueryEntities command, final Map<ADBKey, ADBEntityType> data) {
        return Behaviors.setup(context ->
                new ADBQuerySelectHandler(context, command.getRespondTo(), command.getTransactionId(), command.getQuery(), data));
    }
}
