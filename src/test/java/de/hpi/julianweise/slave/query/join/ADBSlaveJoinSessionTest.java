package de.hpi.julianweise.slave.query.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query.join.ADBMasterJoinSession;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySessionFactory;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBSlaveJoinSessionTest {

    public static final int TRANSACTION_ID = 1;

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
    public void expectRequestForNextShardComparisonAfterExecuteCommand() {
        TestProbe<ADBMasterQuerySession.Command> querySession = testKit.createTestProbe();
        TestProbe<ADBLargeMessageReceiver.InitializeTransfer> initializeTransferTestProbe = testKit.createTestProbe();

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ADBQueryManager.QueryEntities queryCommand = ADBQueryManager.QueryEntities.builder()
                                                                           .query(joinQuery)
                                                                           .respondTo(querySession.ref())
                                                                           .clientLargeMessageReceiver(initializeTransferTestProbe.ref())
                                                                           .transactionId(TRANSACTION_ID)
                                                                           .build();

        ActorRef<ADBSlaveJoinSession.Command> joinHandler = testKit.spawn(ADBSlaveQuerySessionFactory
                .createForJoinQuery(queryCommand));

        joinHandler.tell(new ADBSlaveQuerySession.Execute());

        ADBMasterQuerySession.RegisterQuerySessionHandler handlerRegistration =
                querySession.expectMessageClass(ADBMasterQuerySession.RegisterQuerySessionHandler.class);

        assertThat(handlerRegistration.getSessionHandler()).isEqualTo(joinHandler);
        assertThat(handlerRegistration.getQueryManager()).isEqualTo(ADBQueryManager.getInstance());

        joinHandler.tell(new ADBSlaveJoinSession.RequestNextPartition());

        ADBMasterJoinSession.RequestNextNodeToJoin request =
                querySession.expectMessageClass(ADBMasterJoinSession.RequestNextNodeToJoin.class);

        assertThat(request.getRespondTo()).isEqualTo(joinHandler);
    }
}