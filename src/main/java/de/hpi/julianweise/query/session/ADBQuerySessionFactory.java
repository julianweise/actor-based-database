package de.hpi.julianweise.query.session;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.query.session.join.ADBJoinQuerySession;
import de.hpi.julianweise.query.session.select.ADBSelectQuerySession;
import de.hpi.julianweise.shard.ADBShard;

import java.util.List;

public class ADBQuerySessionFactory {

    public static Behavior<ADBQuerySession.Command> create(List<ActorRef<ADBShard.Command>> shards, ADBQuery query,
                                                           int transactionId,
                                                           ActorRef<ADBShardInquirer.Command> parent) {
        if (query instanceof ADBSelectionQuery) {
            return ADBQuerySessionFactory.createSelectSession(shards, transactionId, parent, (ADBSelectionQuery) query);
        } else if (query instanceof ADBJoinQuery) {
            return ADBQuerySessionFactory.createJoinSession(shards, transactionId, parent, (ADBJoinQuery) query);
        }
        return Behaviors.same();
    }

    private static Behavior<ADBQuerySession.Command> createSelectSession(List<ActorRef<ADBShard.Command>> shards,
                                                                         int transactionId,
                                                                         ActorRef<ADBShardInquirer.Command> parent,
                                                                         ADBSelectionQuery query) {
        return Behaviors.setup(context -> new ADBSelectQuerySession(context, shards, transactionId, parent, query));
    }

    private static Behavior<ADBQuerySession.Command> createJoinSession(List<ActorRef<ADBShard.Command>> shards,
                                                                       int transactionId,
                                                                       ActorRef<ADBShardInquirer.Command> parent,
                                                                       ADBJoinQuery query) {
        return Behaviors.setup(context -> new ADBJoinQuerySession(context, shards, transactionId, parent, query));
    }

    public static String sessionName(ADBQuery query, int transactionId) {
        if (query instanceof ADBJoinQuery) {
            return "ADBJoinQuerySession" + "-for-" + transactionId;
        }
        else if (query instanceof ADBSelectQuerySession) {
            return "ADBSelectQuerySession" + "-for-" + transactionId;
        }
        return "UnspecifiedQuerySession" + "-for-" + transactionId;
    }
}
