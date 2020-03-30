package de.hpi.julianweise.shard.query_operation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.query_operation.join.ADBJoinQuerySessionHandler;

import java.util.List;

public class ADBQuerySessionHandlerFactory {

    public static Behavior<ADBQuerySessionHandler.Command> create(ADBShard.QueryEntities command,
                                                                  ActorRef<ADBShard.Command> shard,
                                                                  final List<ADBEntityType> data,
                                                                  int globalShardId) {
        if (command.getQuery() instanceof ADBSelectionQuery) {
            return ADBQuerySessionHandlerFactory.createForSelectionQuery(command, shard, data, globalShardId);
        } else if (command.getQuery() instanceof ADBJoinQuery) {
            return ADBQuerySessionHandlerFactory.createForJoinQuery(command, shard, data, globalShardId);
        }
        return Behaviors.same();
    }

    public static Behavior<ADBQuerySessionHandler.Command> createForSelectionQuery(ADBShard.QueryEntities command,
                                                                                   ActorRef<ADBShard.Command> shard,
                                                                                   final List<ADBEntityType> data,
                                                                                   int globalShardId) {
        return Behaviors.setup(context ->
                new ADBSelectQuerySessionHandler(context, shard, command.getRespondTo(),
                        command.getClientLargeMessageReceiver(), command.getTransactionId(),
                        (ADBSelectionQuery) command.getQuery(), data, globalShardId));
    }

    public static Behavior<ADBQuerySessionHandler.Command> createForJoinQuery(ADBShard.QueryEntities command,
                                                                              ActorRef<ADBShard.Command> shard,
                                                                              final List<ADBEntityType> data,
                                                                              int globalShardId) {
        return Behaviors.setup(context ->
                new ADBJoinQuerySessionHandler(context, shard, command.getRespondTo(),
                        command.getClientLargeMessageReceiver(), command.getTransactionId(),
                        (ADBJoinQuery) command.getQuery(), data, globalShardId));
    }

    public static String sessionHandlerName(ADBShard.QueryEntities command, int globalShardId) {
        if (command.getQuery() instanceof ADBJoinQuery) {
            return "ADBJoinQuerySessionHandler" + "-for-" + command.getTransactionId() + "-on-shard-" + globalShardId;
        }
        else if (command.getQuery() instanceof ADBSelectionQuery) {
            return "ADBSelectQuerySessionHandler" + "-for-" + command.getTransactionId() + "-on-shard-" + globalShardId;
        }
        return "UnspecifiedQuerySessionHandler" + "-for-" + command.getTransactionId() + "-on-shard-" + globalShardId;
    }
}