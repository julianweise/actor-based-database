package de.hpi.julianweise.shard.queryOperation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;

import java.util.Map;

public class ADBQueryJoinHandler extends ADBQueryOperationHandler {

    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());

    public ADBQueryJoinHandler(ActorContext<Command> context,
                               ActorRef<ADBShardInquirer.Command> client, int transactionId,
                               ADBJoinQuery query, final Map<ADBKey, ADBEntityType> data) {
        super(context, client, transactionId, query, data);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Execute.class, this::handleExecute)
                .build();
    }

    private Behavior<Command> handleExecute(Execute command) {

        return this.concludeTransaction();
    }

}
