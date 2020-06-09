package de.hpi.julianweise.domain.custom.sfemployeesalary;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class SFEmployeeSalaryDeserializer extends JsonDeserializer<SFEmployeeSalary> {

    @Override
    public SFEmployeeSalary deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);

        return SFEmployeeSalary
                .builder()
                .yearType(node.get("yearType").asText())
                .year(node.get("year").asInt())
                .organizationGroupCode(node.get("organizationGroupCode").asInt())
                .organizationGroup(node.get("organizationGroup").asText())
                .departmentCode(node.get("departmentCode").asText())
                .department(node.get("department").asText())
                .unionCode(node.get("unionCode").asText())
                .union(node.get("union").asText())
                .jobFamilyCode(node.get("jobFamilyCode").asText())
                .jobFamily(node.get("jobFamily").asText())
                .jobCode(node.get("jobCode").asText())
                .job(node.get("job").asText())
                .employeeIdentifier(node.get("employeeIdentifier").asInt())
                .salaries(node.get("salaries").asDouble())
                .overtime(node.get("overtime").asDouble())
                .otherSalaries(node.get("otherSalaries").asDouble())
                .totalSalary(node.get("totalSalary").asDouble())
                .retirement(node.get("retirement").asDouble())
                .healthAndDental(node.get("healthAndDental").asDouble())
                .otherBenefits(node.get("otherBenefits").asDouble())
                .totalBenefits(node.get("totalBenefits").asDouble())
                .totalCompensation(node.get("totalCompensation").asDouble())
                .build();

    }
}
