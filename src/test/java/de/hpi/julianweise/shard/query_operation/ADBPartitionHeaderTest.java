package de.hpi.julianweise.shard.query_operation;

import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.shard.revisited.ADBPartitionHeader;
import de.hpi.julianweise.shard.revisited.ADBPartitionHeaderFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class ADBPartitionHeaderTest {

    @Test
    public void expectCorrectCreation() {
        List<ADBEntity> data = new ArrayList<>();
        data.add(new TestEntity(-1, "T", -1.0f, true, -1.01, 'a'));
        data.add(new TestEntity(1, "Te", 1.0f, false, 1.0, 'b'));
        data.add(new TestEntity(4, "Tes", 1.4f, true, 1.04, 'c'));
        data.add(new TestEntity(8, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader header = ADBPartitionHeaderFactory.createDefault(data);

        assertThat(header.getMinValues().size()).isEqualTo(6);
        assertThat(header.getMaxValues().size()).isEqualTo(6);

        assertThat(header.getMinValues().get("aInteger")).isEqualTo(0);
        assertThat(header.getMaxValues().get("aInteger")).isEqualTo(3);
        assertThat(header.getMinValues().get("bString")).isEqualTo(0);
        assertThat(header.getMaxValues().get("bString")).isEqualTo(3);
        assertThat(header.getMinValues().get("cFloat")).isEqualTo(0);
        assertThat(header.getMaxValues().get("cFloat")).isEqualTo(3);
        assertThat(header.getMinValues().get("dBoolean")).isEqualTo(1);
        assertThat(header.getMaxValues().get("dBoolean")).isEqualTo(0);
        assertThat(header.getMinValues().get("eDouble")).isEqualTo(0);
        assertThat(header.getMaxValues().get("eDouble")).isEqualTo(3);
        assertThat(header.getMinValues().get("fChar")).isEqualTo(0);
        assertThat(header.getMaxValues().get("fChar")).isEqualTo(3);
    }

    @Test
    public void expectCorrectOverlappingForLess() {
        List<ADBEntity> dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(-1, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(1, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(4, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(8, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        List<ADBEntity> dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(-11, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(-21, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(-41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(-81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(22, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

    @Test
    public void expectCorrectOverlappingForLessOrEqual() {
        List<ADBEntity> dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        List<ADBEntity> dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(24, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

    @Test
    public void expectCorrectOverlappingForGreater() {
        List<ADBEntity> dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(-1, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(1, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(4, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(8, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        List<ADBEntity> dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.GREATER, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(-11, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(-21, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(-41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(-81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();

        dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(22, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(123, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(131, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(141, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(181, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();
    }

    @Test
    public void expectCorrectOverlappingForGreaterOrEqual() {
        List<ADBEntity> dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        List<ADBEntity> dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(22, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));


        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

    @Test
    public void expectCorrectOverlappingForEqual() {
        List<ADBEntity> dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        List<ADBEntity> dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(22, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft);

        dataRight = new ArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));


        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

}