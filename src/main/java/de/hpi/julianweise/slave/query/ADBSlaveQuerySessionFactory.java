package de.hpi.julianweise.slave.query;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
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

    public static Behavior<ADBSlaveQuerySession.Command> createSelectionQuery(ADBQueryManager.QueryEntities command) {
        return Behaviors.setup(context ->
                new ADBSlaveSelectSession(context, command.getRespondTo(),
                        command.getClientLargeMessageReceiver(), command.getTransactionId(),
                        (ADBSelectionQuery) command.getQuery()));
    }

    public static Behavior<ADBSlaveQuerySession.Command> createForJoinQuery(ADBQueryManager.QueryEntities command) {
        return Behaviors.setup(context ->
                new ADBSlaveJoinSession(context, command.getRespondTo(), command.getClientLargeMessageReceiver(),
                        command.getTransactionId(), (ADBJoinQuery) command.getQuery()));
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
