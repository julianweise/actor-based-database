package de.hpi.julianweise.shard.queryOperation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ADBQuerySelectHandler extends ADBQueryOperationHandler {

    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());


    public ADBQuerySelectHandler(ActorContext<ADBQueryOperationHandler.Command> context,
                                 ActorRef<ADBShardInquirer.Command> client, int transactionId,
                                 ADBSelectionQuery query, final List<ADBEntityType> data) {
        super(context, client, transactionId, query, data);
    }

    @Override
    public Receive<ADBQueryOperationHandler.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Execute.class, this::handleExecute)
                .build();
    }

    private Behavior<ADBQueryOperationHandler.Command> handleExecute(Execute command) {
        final AtomicInteger counter = new AtomicInteger();
        Collection<List<ADBEntityType>> results = this.data.stream()
                                                           .filter(entity -> entity.matches((ADBSelectionQuery) this.query))
                                                           .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / this.settings.QUERY_RESPONSE_CHUNK_SIZE))
                                                           .values();

        results.forEach(chunk -> this.client.tell(new ADBShardInquirer.QueryResults(transactionId, chunk)));
        return this.concludeTransaction();
    }

}
