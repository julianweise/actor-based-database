package de.hpi.julianweise.shard.query_operation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.query.session.select.ADBSelectQuerySession;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.query_operation.join.attribute_comparison.ADBJoinAttributeComparator;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;

import java.util.List;
import java.util.stream.Collectors;

public class ADBSelectQuerySessionHandler extends ADBQuerySessionHandler {

    public ADBSelectQuerySessionHandler(ActorContext<ADBQuerySessionHandler.Command> context,
                                        ActorRef<ADBShard.Command> shard,
                                        ActorRef<ADBQuerySession.Command> client,
                                        ActorRef<ADBLargeMessageReceiver.InitializeTransfer> clientLargeMessageReceiver,
                                        ActorRef<ADBJoinAttributeComparator.Command> comparatorPool,
                                        int transactionId,
                                        ADBSelectionQuery query,
                                        final List<ADBEntity> data,
                                        int globalShardId) {
        super(context, shard, client, clientLargeMessageReceiver, comparatorPool, transactionId, query, data,
                globalShardId);
    }

    @Override
    public Receive<ADBQuerySessionHandler.Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(Execute.class, this::handleExecute)
                   .build();
    }

    private Behavior<ADBQuerySessionHandler.Command> handleExecute(Execute command) {
        List<ADBEntity> results = this.data
                .stream()
                .filter(entity -> entity.matches((ADBSelectionQuery) this.query))
                .collect(Collectors.toList());

        this.sendToSession(ADBSelectQuerySession.SelectQueryResults.builder()
                                                                   .results(results)
                                                                   .globalShardId(this.globalShardId)
                                                                   .transactionId(transactionId)
                                                                   .build(), results.size());
        this.concludeTransaction();
        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Select Query";
    }

}
