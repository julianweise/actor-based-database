package de.hpi.julianweise.query.session.select;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.query.session.join.JoinDistributionPlan;
import de.hpi.julianweise.shard.ADBShard;

import java.util.ArrayList;
import java.util.Set;

public class ADBSelectQuerySession extends ADBQuerySession {

    public ADBSelectQuerySession(ActorContext<Command> context, Set<ActorRef<ADBShard.Command>> shards,
                                    int transactionId, ActorRef<ADBShardInquirer.Command> parent, ADBSelectionQuery query) {
        super(context, shards, transactionId, parent);
        // Send initial query
        this.shards.forEach(shard -> shard.tell(ADBShard.QueryEntities.builder()
                                                                      .transactionId(transactionId)
                                                                      .query(query)
                                                                      .respondTo(this.getContext().getSelf())
                                                                      .build()));
    }

    @Override
    public Receive<Command> createReceive() {
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
        this.shards.remove(response.getShard());
        if (this.shards.isEmpty()) {
            return this.concludeTransaction();
        }
        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Select Query";
    }
}
