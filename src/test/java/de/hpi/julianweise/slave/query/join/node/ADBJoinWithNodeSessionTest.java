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
        int leftNodeId = 1;
        int rightNodeId = 2;
        int lPartitionId = 0;

        TestProbe<ADBSlaveQuerySession.Command> left = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> remotePartitionManager = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> partitionManager = testKit.createTestProbe();
        ADBPartitionManager.resetSingleton();
        ADBPartitionManager.setInstance(partitionManager.ref());

        List<ADBJoinQueryPredicate> joinPredicates = new ArrayList<>();
        joinPredicates.add(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "cFloat"));
        joinPredicates.add(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "eDouble"));
        ADBJoinQuery joinQuery = new ADBJoinQuery(joinPredicates, false);

        ADBJoinQueryContext context = new ADBJoinQueryContext(joinQuery, transactionId);
        ADBJoinNodesContext joinNodesContext = ADBJoinNodesContext.builder()
                                                                  .right(partitionManager.ref())
                                                                  .left(remotePartitionManager.ref())
                                                                  .leftNodeId(leftNodeId)
                                                                  .rightNodeId(rightNodeId)
                                                                  .build();

        String name = ADBJoinWithNodeSessionFactory.sessionName(transactionId, joinNodesContext);
        ActorRef<ADBJoinWithNodeSession.Command> session = testKit.spawn(ADBJoinWithNodeSessionFactory
                .createDefault(context, left.ref(), joinNodesContext), name);

        remotePartitionManager.expectMessageClass(ADBPartitionManager.RequestAllPartitionHeaders.class);

        ObjectList<ADBPartitionHeader> headers = new ObjectArrayList<>();
        headers.add(new ADBPartitionHeader(Collections.emptyMap(), Collections.emptyMap(), new ObjectArrayList<>(), lPartitionId));

        session.tell(new ADBJoinWithNodeSession.AllPartitionsHeaderWrapper(new ADBPartitionManager.AllPartitionsHeaders(headers)));

        ADBPartitionManager.RequestPartitionsForJoinQuery request =
                partitionManager.expectMessageClass(ADBPartitionManager.RequestPartitionsForJoinQuery.class);

        assertThat(request.getExternalHeader()).isEqualTo(headers.get(0));

        int[] lRemotePartitionIds = {0};
        int[] rRemotePartitionIds = {0};
        session.tell(new ADBJoinWithNodeSession.RelevantPartitionsWrapper(new ADBPartitionManager
                .RelevantPartitionsJoinQuery(0, lRemotePartitionIds, rRemotePartitionIds)));

        ADBPartitionManager.RedirectToPartition redirectCommand11 =
                partitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        ADBPartitionManager.RedirectToPartition redirectCommand21 =
                remotePartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        Map<String, ObjectList<ADBEntityEntry>> attributes = new HashMap<>();
        attributes.put("aInteger", new ObjectArrayList<>());
        attributes.put("cFloat", new ObjectArrayList<>());
        attributes.put("eDouble", new ObjectArrayList<>());

        redirectCommand11.getMessage().getRespondTo().tell(new ADBPartition.MultipleAttributes(attributes));
        redirectCommand21.getMessage().getRespondTo().tell(new ADBPartition.MultipleAttributes(attributes));

        ADBPartitionManager.RedirectToPartition redirectCommand12 =
                partitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        ADBPartitionManager.RedirectToPartition redirectCommand22 =
                remotePartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(redirectCommand11.getMessage().getAttributes()).containsExactly("aInteger");
        assertThat(redirectCommand21.getMessage().getAttributes()).containsExactly("aInteger");
        assertThat(redirectCommand12.getMessage().getAttributes()).containsExactly("cFloat", "eDouble");
        assertThat(redirectCommand22.getMessage().getAttributes()).containsExactly("cFloat", "eDouble");
    }

}