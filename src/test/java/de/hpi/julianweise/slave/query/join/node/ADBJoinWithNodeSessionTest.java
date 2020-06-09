package de.hpi.julianweise.slave.query.join.node;


import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBJoinQueryContext;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBJoinWithNodeSessionTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
        ADBComparator.buildComparatorMapping();
        testKit.spawn(ADBSlave.create());
    }

    @After
    public void cleanup() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
        testKit = new TestKitJunitResource();
    }

    @AfterClass
    public static void after() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
    }

    @Test
    public void testCorrectJoin() {
        int transactionId = 99;
        int remoteNodeId = 2;
        int lPartitionId = 0;

        TestProbe<ADBSlaveQuerySession.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> remotePartitionManager = testKit.createTestProbe();
        TestProbe<ADBJoinWithNodeSessionHandler.Command> sessionHandler = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> partitionManager = testKit.createTestProbe();
        ADBPartitionManager.resetSingleton();
        ADBPartitionManager.setInstance(partitionManager.ref());

        List<ADBJoinQueryPredicate> joinPredicates = new ArrayList<>();
        joinPredicates.add(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "cFloat"));
        joinPredicates.add(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "eDouble"));
        ADBJoinQuery joinQuery = new ADBJoinQuery(joinPredicates, false);

        ADBJoinQueryContext context = new ADBJoinQueryContext(joinQuery, transactionId);

        String name = ADBJoinWithNodeSessionFactory.sessionName(transactionId, remoteNodeId);
        ActorRef<ADBJoinWithNodeSession.Command> session = testKit.spawn(ADBJoinWithNodeSessionFactory
                .createDefault(context, supervisor.ref(), remoteNodeId), name);

        partitionManager.expectMessageClass(ADBPartitionManager.RequestAllPartitionHeaders.class);

        session.tell(new ADBJoinWithNodeSession.RegisterHandler(sessionHandler.ref(), remotePartitionManager.ref()));
        ObjectList<ADBPartitionHeader> headers = new ObjectArrayList<>();
        headers.add(new ADBPartitionHeader(Collections.emptyMap(), Collections.emptyMap(), new ObjectArrayList<>(), lPartitionId));

        session.tell(new ADBJoinWithNodeSession.AllPartitionsHeaderWrapper(new ADBPartitionManager.AllPartitionsHeaders(headers)));

        ADBJoinWithNodeSessionHandler.ProvideRelevantJoinPartitions request =
                sessionHandler.expectMessageClass(ADBJoinWithNodeSessionHandler.ProvideRelevantJoinPartitions.class);

        assertThat(request.getHeader()).isEqualTo(headers.get(0));

        int[] lRemotePartitionIds = {0};
        int[] rRemotePartitionIds = {0};
        session.tell(new ADBJoinWithNodeSession.RelevantJoinPartitions(lPartitionId, lRemotePartitionIds, rRemotePartitionIds));

        ADBPartitionManager.RedirectToPartition redirectCommand11 =
                remotePartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        ADBPartitionManager.RedirectToPartition redirectCommand21 =
                partitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        Map<String, ObjectList<ADBEntityEntry>> attributes = new HashMap<>();
        attributes.put("aInteger", new ObjectArrayList<>());
        attributes.put("cFloat", new ObjectArrayList<>());
        attributes.put("eDouble", new ObjectArrayList<>());

        redirectCommand11.getMessage().getRespondTo().tell(new ADBPartition.MultipleAttributes(attributes));
        redirectCommand21.getMessage().getRespondTo().tell(new ADBPartition.MultipleAttributes(attributes));

        ADBPartitionManager.RedirectToPartition redirectCommand12 =
                remotePartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        ADBPartitionManager.RedirectToPartition redirectCommand22 =
                partitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(redirectCommand11.getMessage().getAttributes()).containsExactly("aInteger");
        assertThat(redirectCommand21.getMessage().getAttributes()).containsExactly("aInteger");
        assertThat(redirectCommand12.getMessage().getAttributes()).containsExactly("cFloat", "eDouble");
        assertThat(redirectCommand22.getMessage().getAttributes()).containsExactly("cFloat", "eDouble");
    }

}