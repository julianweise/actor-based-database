package de.hpi.julianweise.shard.queryOperation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.shard.ADBShard;

import java.util.List;

public class ADBQuerySessionHandlerFactory {

    public static Behavior<ADBQuerySessionHandler.Command> create(ADBShard.QueryEntities command,
                                                                  ActorRef<ADBShard.Command> shard,
                                                                  final List<ADBEntityType> data) {
        if (command.getQuery() instanceof ADBSelectionQuery) {
            return ADBQuerySessionHandlerFactory.createSelectionQuery(command, shard, data);
        } else if (command.getQuery() instanceof ADBJoinQuery) {
            return ADBQuerySessionHandlerFactory.createJoinQuery(command, shard, data);
        }
        return Behaviors.same();
    }

    public static Behavior<ADBQuerySessionHandler.Command> createSelectionQuery(ADBShard.QueryEntities command,
                                                                                ActorRef<ADBShard.Command> shard,
                                                                                final List<ADBEntityType> data) {
        return Behaviors.setup(context ->
                new ADBSelectQuerySessionHandler(context, shard, command.getRespondTo(), command.getTransactionId(),
                        (ADBSelectionQuery) command.getQuery(), data));
    }

    public static Behavior<ADBQuerySessionHandler.Command> createJoinQuery(ADBShard.QueryEntities command,
                                                                           ActorRef<ADBShard.Command> shard,
                                                                           final List<ADBEntityType> data) {
        return Behaviors.setup(context ->
                new ADBJoinQuerySessionHandler(context, shard, command.getRespondTo(), command.getTransactionId(),
                        (ADBJoinQuery) command.getQuery(), data));
    }
}
