package de.hpi.julianweise.query.session.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.shard.ADBShard;

import java.util.Set;

public class ADBJoinQuerySession extends ADBQuerySession {

    public ADBJoinQuerySession(ActorContext<ADBQuerySession.Command> context,
                                  Set<ActorRef<ADBShard.Command>> shards, int transactionId,
                                  ActorRef<ADBShardInquirer.Command> parent,
                                  ADBJoinQuery query) {
        super(context, shards, transactionId, parent);

        // Send initial query
        this.shards.forEach(shard -> shard.tell(ADBShard.QueryEntities.builder()
                                                                      .transactionId(transactionId)
                                                                      .query(query)
                                                                      .respondTo(this.getContext().getSelf())
                                                                      .build()));
    }

    @Override
    public Receive<ADBQuerySession.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(QueryResults.class, this::handleQueryResults)
                .onMessage(ConcludeTransaction.class, this::handleConcludeTransaction)
                .build();
    }

    private Behavior<ADBQuerySession.Command> handleQueryResults(QueryResults response) {
        this.queryResults.addAll(response.getResults());
        return Behaviors.same();
    }

    private Behavior<ADBQuerySession.Command> handleConcludeTransaction(ConcludeTransaction response) {

        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Join Query";
    }
}
