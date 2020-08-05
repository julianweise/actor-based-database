package de.hpi.julianweise.slave.query.join.node;


import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBDoubleColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBFloatColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBIntColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityDoubleEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityFloatEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityIntEntry;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBJoinQueryContext;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBNodeJoinTest {

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
    public void testCorrectJoin() throws NoSuchFieldException {
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
        ADBNodeJoinContext joinNodesContext = ADBNodeJoinContext.builder()
                                                                .right(partitionManager.ref())
                                                                .left(remotePartitionManager.ref())
                                                                .leftNodeId(leftNodeId)
                                                                .rightNodeId(rightNodeId)
                                                                .build();

        String name = ADBNodeJoinFactory.sessionName(joinNodesContext);
        ActorRef<ADBNodeJoin.Command> session = testKit.spawn(ADBNodeJoinFactory
                .createDefault(context, left.ref(), joinNodesContext), name);

        remotePartitionManager.expectMessageClass(ADBPartitionManager.RequestAllPartitionHeaders.class);

        ObjectList<ADBPartitionHeader> headers = new ObjectArrayList<>();

        Object2ObjectOpenHashMap<String, ADBEntityEntry> minValues = new Object2ObjectOpenHashMap<>();
        Object2ObjectOpenHashMap<String, ADBEntityEntry> maxValues = new Object2ObjectOpenHashMap<>();

        ADBEntity entity = new TestEntity(1, "Test", 1f, true, 1.00);
        Field intField = TestEntity.class.getDeclaredField("aInteger");
        minValues.put("aInteger", new ADBEntityIntEntry(1, intField, entity));
        maxValues.put("aInteger", new ADBEntityIntEntry(1, intField, entity));

        Field floatField = TestEntity.class.getDeclaredField("cFloat");
        minValues.put("cFloat", new ADBEntityFloatEntry(1, floatField, entity));
        maxValues.put("cFloat", new ADBEntityFloatEntry(1, floatField, entity));

        Field doubleField = TestEntity.class.getDeclaredField("eDouble");
        minValues.put("eDouble", new ADBEntityDoubleEntry(1, doubleField, entity));
        maxValues.put("eDouble", new ADBEntityDoubleEntry(1, doubleField, entity));

        headers.add(new ADBPartitionHeader(minValues, maxValues, lPartitionId));

        session.tell(new ADBNodeJoin.AllPartitionsHeaderWrapper(new ADBPartitionManager.AllPartitionsHeaders(headers)));

        ADBPartitionManager.RequestPartitionsForJoinQuery request =
                partitionManager.expectMessageClass(ADBPartitionManager.RequestPartitionsForJoinQuery.class);

        assertThat(request.getExternalHeader()).isEqualTo(headers.get(0));

        int[] lRemotePartitionIds = {0};
        int[] rRemotePartitionIds = {0};
        Int2ObjectOpenHashMap<ADBPartitionHeader> allHeaders = new Int2ObjectOpenHashMap<>();
        allHeaders.put(0, new ADBPartitionHeader(minValues, maxValues, lPartitionId));
        session.tell(new ADBNodeJoin.RelevantPartitionsWrapper(new ADBPartitionManager
                .RelevantPartitionsJoinQuery(0, lRemotePartitionIds, allHeaders, rRemotePartitionIds)));

        ADBPartitionManager.RedirectToPartition redirectCommand11 =
                partitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        ADBPartitionManager.RedirectToPartition redirectCommand21 =
                remotePartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        Map<String, ADBColumnSorted> attributes = new HashMap<>();
        attributes.put("aInteger", new ADBIntColumnSorted(0, 0, new int[]{1}, new int[]{0}, new int[]{0}));
        attributes.put("cFloat", new ADBFloatColumnSorted(0, 0, new float[]{1f}, new int[]{0}, new int[]{0}));
        attributes.put("eDouble", new ADBDoubleColumnSorted(0, 0, new double[]{1.00}, new int[]{0}, new int[]{0}));

        Object2IntMap<String> originalSizes = new Object2IntOpenHashMap<>();
        originalSizes.put("aInteger", 1);
        originalSizes.put("cFloat", 1);
        originalSizes.put("eDouble", 1);

        redirectCommand11.getMessage().getRespondTo().tell(new ADBPartition.MultipleAttributes(attributes,
                originalSizes, true));
        redirectCommand21.getMessage().getRespondTo().tell(new ADBPartition.MultipleAttributes(attributes,
                originalSizes, false));

        ADBPartitionManager.RedirectToPartition redirectCommand12 =
                partitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        ADBPartitionManager.RedirectToPartition redirectCommand22 =
                remotePartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(redirectCommand11.getMessage().getAttributes()).containsAnyOf("aInteger", "cFloat", "eDouble");
        assertThat(redirectCommand12.getMessage().getAttributes()).containsAnyOf("aInteger", "cFloat", "eDouble");
        assertThat(redirectCommand21.getMessage().getAttributes()).containsAnyOf("aInteger", "cFloat", "eDouble");
        assertThat(redirectCommand22.getMessage().getAttributes()).containsAnyOf("aInteger", "cFloat", "eDouble");
    }

}