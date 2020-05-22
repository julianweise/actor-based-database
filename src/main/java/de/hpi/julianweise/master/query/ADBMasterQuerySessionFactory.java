package de.hpi.julianweise.master.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.master.query.join.ADBMasterJoinSession;
import de.hpi.julianweise.master.query.select.ADBMasterSelectSession;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirer;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ADBMasterQuerySessionFactory {

    private final static Logger LOG = LoggerFactory.getLogger(ADBMasterQuerySessionFactory.class);

    public static Behavior<ADBMasterQuerySession.Command> create(ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers,
                                                                 ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers,
                                                                 ADBQuery query, int transactionId,
                                                                 ActorRef<ADBPartitionInquirer.Command> parent) {
        if (query instanceof ADBSelectionQuery) {
            return ADBMasterQuerySessionFactory.createSelectSession(queryManagers, partitionManagers, transactionId,
                    parent,
                    (ADBSelectionQuery) query);
        } else if (query instanceof ADBJoinQuery) {
            return ADBMasterQuerySessionFactory.createJoinSession(queryManagers, partitionManagers, transactionId,
                    parent,
                    (ADBJoinQuery) query);
        }
        LOG.error("Received unknown query type {}", query.getClass().getSimpleName());
        return Behaviors.same();
    }

    private static Behavior<ADBMasterQuerySession.Command> createSelectSession(ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers,
                                                                               ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers,
                                                                               int transactionId,
                                                                               ActorRef<ADBPartitionInquirer.Command> parent,
                                                                               ADBSelectionQuery query) {
        return Behaviors.setup(context
                -> new ADBMasterSelectSession(context, queryManagers, partitionManagers, transactionId, parent, query));
    }

    private static Behavior<ADBMasterQuerySession.Command> createJoinSession(ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers,
                                                                             ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers,
                                                                             int transactionId,
                                                                             ActorRef<ADBPartitionInquirer.Command> parent,
                                                                             ADBJoinQuery query) {
        return Behaviors.setup(context
                -> new ADBMasterJoinSession(context, queryManagers, partitionManagers, transactionId, parent, query));
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
