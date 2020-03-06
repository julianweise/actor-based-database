package de.hpi.julianweise.shard.queryOperation;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.shard.ADBShard;

import java.util.Map;

public class ADBQueryOperationHandlerFactory {

    public static Behavior<ADBQueryOperationHandler.Command> create(ADBShard.QueryEntities command,
                                                                    final Map<ADBKey, ADBEntityType> data) {
        if (command.getQuery() instanceof ADBSelectionQuery) {
            return ADBQueryOperationHandlerFactory.createSelectionQuery(command, data);
        } else if (command.getQuery() instanceof ADBJoinQuery) {
            return ADBQueryOperationHandlerFactory.createJoinQuery(command, data);
        }
        return Behaviors.same();
    }

    public static Behavior<ADBQueryOperationHandler.Command> createSelectionQuery(ADBShard.QueryEntities command,
                                                                    final Map<ADBKey, ADBEntityType> data) {
        return Behaviors.setup(context ->
                new ADBQuerySelectHandler(context, command.getRespondTo(), command.getTransactionId(),
                        (ADBSelectionQuery) command.getQuery(), data));
    }

    public static Behavior<ADBQueryOperationHandler.Command> createJoinQuery(ADBShard.QueryEntities command,
                                                                    final Map<ADBKey, ADBEntityType> data) {
        return Behaviors.setup(context ->
                new ADBQueryJoinHandler(context, command.getRespondTo(), command.getTransactionId(),
                        (ADBJoinQuery) command.getQuery(), data));
    }
}
