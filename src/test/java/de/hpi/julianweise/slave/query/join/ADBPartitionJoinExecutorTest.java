package de.hpi.julianweise.slave.query.join;


import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.agrona.collections.Object2ObjectHashMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unchecked")
public class ADBPartitionJoinExecutorTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
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
    public void expectEqualityJoinToBeExecutedSuccessfully() {
        int lPartitionId = 0;

        TestProbe<ADBPartition.Command> localPartition = testKit.createTestProbe();
        TestProbe<ADBPartitionJoinExecutor.PartitionsJoined> supervisor = testKit.createTestProbe();

        Map<String, ObjectList<ADBComparable2IntPair>> foreignAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBComparable2IntPair> fIntegerAttributes = new ObjectArrayList<>();
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 1, 0));
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 3, 1));
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 5, 2));
        foreignAttributes.put("aInteger", fIntegerAttributes);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        Behavior<ADBPartitionJoinExecutor.Command> behavior = ADBPartitionJoinExecutorFactory.createDefault(joinQuery,
                localPartition.ref(), foreignAttributes, supervisor.ref(), false);
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        ADBPartition.RequestJoinAttributes requestJoinAttributes =
                localPartition.expectMessageClass(ADBPartition.RequestJoinAttributes.class);

        assertThat(requestJoinAttributes.getQuery()).isEqualTo(joinQuery);

        Map<String, ObjectList<ADBComparable2IntPair>> localAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBComparable2IntPair> lIntegerAttributes = new ObjectArrayList<>();
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 3, 0));
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 5, 1));
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 9, 2));
        localAttributes.put("aInteger", lIntegerAttributes);

        executor.tell(new ADBPartitionJoinExecutor.PartitionJoinAttributesWrapper(new ADBPartition.JoinAttributes(localAttributes, lPartitionId)));

        ADBPartitionJoinExecutor.PartitionsJoined response =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.PartitionsJoined.class);

        assertThat(response.getJoinTuples().size()).isEqualTo(2);
        assertThat(response.getJoinTuples().get(0).getKey()).isEqualTo(1);
        assertThat(response.getJoinTuples().get(0).getValue()).isEqualTo(0);
        assertThat(response.getJoinTuples().get(1).getKey()).isEqualTo(2);
        assertThat(response.getJoinTuples().get(1).getValue()).isEqualTo(1);
        assertThat(response.isReversed()).isFalse();
    }

    @Test
    public void expectLessReversedJoinToBeExecutedSuccessfully() {
        int lPartitionId = 0;

        TestProbe<ADBPartition.Command> localPartition = testKit.createTestProbe();
        TestProbe<ADBPartitionJoinExecutor.PartitionsJoined> supervisor = testKit.createTestProbe();

        Map<String, ObjectList<ADBComparable2IntPair>> foreignAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBComparable2IntPair> fIntegerAttributes = new ObjectArrayList<>();
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 1, 0));
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 3, 1));
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 5, 2));
        foreignAttributes.put("aInteger", fIntegerAttributes);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));

        Behavior<ADBPartitionJoinExecutor.Command> behavior =
                ADBPartitionJoinExecutorFactory.createDefault(joinQuery.getReverse(),
                        localPartition.ref(), foreignAttributes, supervisor.ref(), true);
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        localPartition.expectMessageClass(ADBPartition.RequestJoinAttributes.class);

        Map<String, ObjectList<ADBComparable2IntPair>> localAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBComparable2IntPair> lIntegerAttributes = new ObjectArrayList<>();
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 3, 0));
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 5, 1));
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 9, 2));
        localAttributes.put("aInteger", lIntegerAttributes);

        executor.tell(new ADBPartitionJoinExecutor.PartitionJoinAttributesWrapper(new ADBPartition.JoinAttributes(localAttributes, lPartitionId)));

        ADBPartitionJoinExecutor.PartitionsJoined response =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.PartitionsJoined.class);

        assertThat(response.getJoinTuples().size()).isEqualTo(1);
        assertThat(response.getJoinTuples().get(0).getKey()).isEqualTo(2);
        assertThat(response.getJoinTuples().get(0).getValue()).isEqualTo(0);
        assertThat(response.isReversed()).isTrue();
    }

    @Test
    public void expectGreaterReversedJoinToBeExecutedSuccessfully() {
        int lPartitionId = 0;

        TestProbe<ADBPartition.Command> localPartition = testKit.createTestProbe();
        TestProbe<ADBPartitionJoinExecutor.PartitionsJoined> supervisor = testKit.createTestProbe();

        Map<String, ObjectList<ADBComparable2IntPair>> foreignAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBComparable2IntPair> fIntegerAttributes = new ObjectArrayList<>();
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 3, 0));
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 5, 1));
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 9, 2));
        foreignAttributes.put("bInteger", fIntegerAttributes);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.GREATER, "aInteger", "bInteger"));

        Behavior<ADBPartitionJoinExecutor.Command> behavior =
                ADBPartitionJoinExecutorFactory.createDefault(joinQuery.getReverse(),
                        localPartition.ref(), foreignAttributes, supervisor.ref(), true);
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        localPartition.expectMessageClass(ADBPartition.RequestJoinAttributes.class);


        Map<String, ObjectList<ADBComparable2IntPair>> localAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBComparable2IntPair> lIntegerAttributes = new ObjectArrayList<>();
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 1, 0));
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 3, 1));
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 5, 2));
        localAttributes.put("aInteger", lIntegerAttributes);

        executor.tell(new ADBPartitionJoinExecutor.PartitionJoinAttributesWrapper(new ADBPartition.JoinAttributes(localAttributes, lPartitionId)));

        ADBPartitionJoinExecutor.PartitionsJoined response =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.PartitionsJoined.class);

        assertThat(response.getJoinTuples().size()).isEqualTo(1);
        assertThat(response.getJoinTuples().get(0).getKey()).isEqualTo(0);
        assertThat(response.getJoinTuples().get(0).getValue()).isEqualTo(2);
        assertThat(response.isReversed()).isTrue();
    }

    @Test
    public void expectJoinWithToTermsToBeExecutedSuccessfully() {
        int lPartitionId = 0;

        TestProbe<ADBPartition.Command> localPartition = testKit.createTestProbe();
        TestProbe<ADBPartitionJoinExecutor.PartitionsJoined> supervisor = testKit.createTestProbe();

        Map<String, ObjectList<ADBComparable2IntPair>> foreignAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBComparable2IntPair> fIntegerAttributes = new ObjectArrayList<>();
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 1, 0));
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 3, 1));
        fIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 6, 2));
        foreignAttributes.put("aInteger", fIntegerAttributes);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL, "aInteger", "aInteger"));
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL, "aInteger", "aInteger"));

        Behavior<ADBPartitionJoinExecutor.Command> behavior = ADBPartitionJoinExecutorFactory.createDefault(joinQuery,
                localPartition.ref(), foreignAttributes, supervisor.ref(), false);
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        ADBPartition.RequestJoinAttributes requestJoinAttributes =
                localPartition.expectMessageClass(ADBPartition.RequestJoinAttributes.class);

        assertThat(requestJoinAttributes.getQuery()).isEqualTo(joinQuery);

        Map<String, ObjectList<ADBComparable2IntPair>> localAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBComparable2IntPair> lIntegerAttributes = new ObjectArrayList<>();
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 3, 0));
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 5, 1));
        lIntegerAttributes.add(new ADBComparable2IntPair((Comparable<Object>) (Comparable<?>) 9, 2));
        localAttributes.put("aInteger", lIntegerAttributes);

        executor.tell(new ADBPartitionJoinExecutor.PartitionJoinAttributesWrapper(new ADBPartition.JoinAttributes(localAttributes, lPartitionId)));

        ADBPartitionJoinExecutor.PartitionsJoined response =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.PartitionsJoined.class);

        assertThat(response.getJoinTuples().size()).isEqualTo(1);
        assertThat(response.getJoinTuples().get(0).getKey()).isEqualTo(1);
        assertThat(response.getJoinTuples().get(0).getValue()).isEqualTo(0);
        assertThat(response.isReversed()).isFalse();
    }


}