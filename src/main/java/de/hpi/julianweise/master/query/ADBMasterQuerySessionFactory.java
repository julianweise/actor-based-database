package de.hpi.julianweise.master.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.master.query.join.ADBMasterJoinSession;
import de.hpi.julianweise.master.query.select.ADBMasterSelectSession;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBPartitionInquirer;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ADBMasterQuerySessionFactory {

    private static final Logger LOG = LogManager.getLogger(ADBMasterQuerySessionFactory.class);

    public static Behavior<ADBMasterQuerySession.Command> create(List<ActorRef<ADBQueryManager.Command>> queryManagers,
                                                                 ADBQuery query, int transactionId,
                                                                 ActorRef<ADBPartitionInquirer.Command> parent) {
        if (query instanceof ADBSelectionQuery) {
            return ADBMasterQuerySessionFactory.createSelectSession(queryManagers, transactionId, parent, (ADBSelectionQuery) query);
        } else if (query instanceof ADBJoinQuery) {
            return ADBMasterQuerySessionFactory.createJoinSession(queryManagers, transactionId, parent, (ADBJoinQuery) query);
        }
        LOG.error("Received unknown query type {}", query.getClass().getSimpleName());
        return Behaviors.same();
    }

    private static Behavior<ADBMasterQuerySession.Command> createSelectSession(List<ActorRef<ADBQueryManager.Command>> queryManagers,
                                                                               int transactionId,
                                                                               ActorRef<ADBPartitionInquirer.Command> parent,
                                                                               ADBSelectionQuery query) {
        return Behaviors.setup(context -> new ADBMasterSelectSession(context, queryManagers, transactionId, parent, query));
    }

    private static Behavior<ADBMasterQuerySession.Command> createJoinSession(List<ActorRef<ADBQueryManager.Command>> queryManagers,
                                                                             int transactionId,
                                                                             ActorRef<ADBPartitionInquirer.Command> parent,
                                                                             ADBJoinQuery query) {
        return Behaviors.setup(context -> new ADBMasterJoinSession(context, queryManagers, transactionId, parent, query));
    }

    public static String sessionName(ADBQuery query, int transactionId) {
        if (query instanceof ADBJoinQuery) {
            return "ADBJoinQuerySession" + "-for-" + transactionId;
        }
        else if (query instanceof ADBSelectionQuery) {
            return "ADBSelectQuerySession" + "-for-" + transactionId;
        }
        return "UnspecifiedQuerySession" + "-for-" + transactionId;
    }
}
