package de.hpi.julianweise.slave.query;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.query.selection.ADBSelectionQuery;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.query.join.ADBJoinQueryContext;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
import de.hpi.julianweise.slave.query.select.ADBSelectQueryContext;
import de.hpi.julianweise.slave.query.select.ADBSlaveSelectSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ADBSlaveQuerySessionFactory {

    private final static Logger LOG = LoggerFactory.getLogger(ADBSlaveQuerySessionFactory.class);

    public static Behavior<ADBSlaveQuerySession.Command> create(ADBQueryManager.QueryEntities command) {
        if (command.getQuery() instanceof ADBSelectionQuery) {
            return ADBSlaveQuerySessionFactory.createSelectionQuery(command);
        } else if (command.getQuery() instanceof ADBJoinQuery) {
            return ADBSlaveQuerySessionFactory.createForJoinQuery(command);
        }
        LOG.error("Received unknown query type {}", command.getQuery().getClass().getSimpleName());
        return Behaviors.same();
    }

    public static Behavior<ADBSlaveQuerySession.Command> createSelectionQuery(ADBQueryManager.QueryEntities cmd) {
        ADBSelectQueryContext queryContext = new ADBSelectQueryContext(cmd.getQuery(), cmd.getTransactionId());
        return Behaviors.setup(ctx ->
                new ADBSlaveSelectSession(ctx, cmd.getRespondTo(), queryContext));
    }

    public static Behavior<ADBSlaveQuerySession.Command> createForJoinQuery(ADBQueryManager.QueryEntities cmd) {
        ADBJoinQueryContext queryContext = new ADBJoinQueryContext(cmd.getQuery(), cmd.getTransactionId());
        return Behaviors.setup(context ->
                new ADBSlaveJoinSession(context, cmd.getRespondTo(), queryContext));
    }

    public static String getName(ADBQueryManager.QueryEntities command) {
        if (command.getQuery() instanceof ADBJoinQuery) {
            return "ADBSlaveJoinSession" + "-for-" + command.getTransactionId() + "-on-shard-" + ADBSlave.ID;
        } else if (command.getQuery() instanceof ADBSelectionQuery) {
            return "ADBSlaveSelectSession" + "-for-" + command.getTransactionId() + "-on-shard-" + ADBSlave.ID;
        }
        return "UnspecifiedQuerySessionHandler" + "-for-" + command.getTransactionId() + "-on-shard-" + ADBSlave.ID;
    }
}
