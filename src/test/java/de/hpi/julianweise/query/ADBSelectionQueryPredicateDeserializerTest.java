package de.hpi.julianweise.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.query.selection.ADBSelectionQueryPredicate;
import de.hpi.julianweise.query.selection.ADBSelectionQueryPredicateDeserializer;
import de.hpi.julianweise.query.selection.constant.ADBPredicateBooleanConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateDoubleConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateFloatConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateIntConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateStringConstant;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBSelectionQueryPredicateDeserializerTest {

    @Test
    public void deserializeInteger() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 4,\n" +
                "  \"fieldName\": \"aInteger\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryPredicateDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQueryPredicate predicate = objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);

        assertThat(predicate.getFieldName()).isEqualTo("aInteger");
        assertThat(predicate.getValue()).isEqualTo(new ADBPredicateIntConstant(4));
    }

    @Test
    public void deserializeString() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": \"TestString\",\n" +
                "  \"fieldName\": \"bString\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryPredicateDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQueryPredicate predicate = objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);

        assertThat(predicate.getFieldName()).isEqualTo("bString");
        assertThat(predicate.getValue()).isEqualTo(new ADBPredicateStringConstant("TestString"));
    }

    @Test
    public void deserializeFloat() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 1.03,\n" +
                "  \"fieldName\": \"cFloat\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryPredicateDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQueryPredicate predicate = objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);

        assertThat(predicate.getFieldName()).isEqualTo("cFloat");
        assertThat(predicate.getValue()).isEqualTo(new ADBPredicateFloatConstant(1.03f));
    }

    @Test
    public void deserializeBoolean() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": true,\n" +
                "  \"fieldName\": \"dBoolean\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryPredicateDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQueryPredicate predicate = objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);

        assertThat(predicate.getFieldName()).isEqualTo("dBoolean");
        assertThat(predicate.getValue()).isEqualTo(new ADBPredicateBooleanConstant(true));
    }

    @Test
    public void deserializeDouble() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 12.0374362372462382432,\n" +
                "  \"fieldName\": \"eDouble\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryPredicateDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQueryPredicate predicate = objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);

        assertThat(predicate.getFieldName()).isEqualTo("eDouble");
        assertThat(predicate.getValue()).isEqualTo(new ADBPredicateDoubleConstant(12.0374362372462382432));
    }

    @Test(expected = NoSuchFieldException.class)
    public void exceptionDuringDeserialization() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 4,\n" +
                "  \"fieldName\": \"invalidField\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryPredicateDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);
    }

}