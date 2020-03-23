package de.hpi.julianweise.domain.custom;

import de.hpi.julianweise.domain.ADBEntityFactory;
import de.hpi.julianweise.domain.ADBEntityType;
import org.apache.commons.csv.CSVRecord;

public class SFEmployeeFactory implements ADBEntityFactory {

    @Override
    public Class<? extends ADBEntityType> getTargetClass() {
        return SFEmployeeSalary.class;
    }

    @Override
    public ADBEntityType build(CSVRecord record) {
        return SFEmployeeSalary.builder()
                               .yearType(record.get(0))
                               .year(Integer.parseInt(record.get(1)))
                               .organizationGroupCode(Integer.parseInt(record.get(2)))
                               .departmentCode(record.get(4))
                               .unionCode(record.get(6))
                               .jobFamilyCode(record.get(8))
                               .jobCode(record.get(10))
                               .employeeIdentifier(Integer.parseInt(record.get(12)))
                               .salaries(Float.parseFloat(record.get(13)))
                               .overtime(Float.parseFloat(record.get(14)))
                               .otherSalaries(Float.parseFloat(record.get(15)))
                               .totalSalary(Float.parseFloat(record.get(16)))
                               .retirement(Float.parseFloat(record.get(17)))
                               .health(Float.parseFloat(record.get(18)))
                               .otherBenefits(Float.parseFloat(record.get(19)))
                               .totalBenefits(Float.parseFloat(record.get(20)))
                               .totalCompensation(Float.parseFloat(record.get(21)))
                               .build();
    }

    @Override
    public SFEmployeeSalaryDeserializer buildDeserializer() {
        return new SFEmployeeSalaryDeserializer();
    }
}