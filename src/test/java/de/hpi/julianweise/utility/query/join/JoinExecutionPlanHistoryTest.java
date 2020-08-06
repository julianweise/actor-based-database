package de.hpi.julianweise.utility.query.join;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JoinExecutionPlanHistoryTest {

    @Test
    public void addEntries() throws InterruptedException {
        int transactionId = 1;
        JoinExecutionPlanHistory history = new JoinExecutionPlanHistory();
        Thread.sleep(50);
        history.logNodeJoin(1,1, 2);
        Thread.sleep(50);
        history.logNodeJoin(1, 1, 3);
        Thread.sleep(100);
        history.logNodeJoin(2,2, 3);

        float averageTimeNode1 = history.getAverageNodeJoinExecutionTime(1);
        assertThat(averageTimeNode1).isGreaterThanOrEqualTo(50);
        assertThat(averageTimeNode1).isLessThanOrEqualTo(55);
        float averageTimeNode2 = history.getAverageNodeJoinExecutionTime(2);
        assertThat(averageTimeNode2).isGreaterThanOrEqualTo(200);
        assertThat(averageTimeNode2).isLessThanOrEqualTo(220);
    }

}