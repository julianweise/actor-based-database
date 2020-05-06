package de.hpi.julianweise.domain.custom;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.domain.ADBEntityFactory;
import org.apache.commons.csv.CSVRecord;

public class SFEmployeeFactory implements ADBEntityFactory {

    @Override
    public Class<? extends ADBEntity> getTargetClass() {
        return SFEmployeeSalary.class;
    }

    @Override
    public ADBEntity build(CSVRecord record) {
        return SFEmployeeSalary.builder()
                               .yearType(record.get(0))
                               .year(Integer.parseInt(record.get(1)))
                               .organizationGroupCode(Integer.parseInt(record.get(2)))
                               .departmentCode(record.get(4))
                               .unionCode(record.get(6))
                               .jobFamilyCode(record.get(8))
                               .jobCode(record.get(10))
                               .employeeIdentifier(Integer.parseInt(record.get(12)))
                               .salaries(Double.parseDouble(record.get(13)))
                               .overtime(Double.parseDouble(record.get(14)))
                               .otherSalaries(Double.parseDouble(record.get(15)))
                               .totalSalary(Double.parseDouble(record.get(16)))
                               .retirement(Double.parseDouble(record.get(17)))
                               .health(Double.parseDouble(record.get(18)))
                               .otherBenefits(Double.parseDouble(record.get(19)))
                               .totalBenefits(Double.parseDouble(record.get(20)))
                               .totalCompensation(Double.parseDouble(record.get(21)))
                               .build();
    }

    @Override
    public SFEmployeeSalaryDeserializer buildDeserializer() {
        return new SFEmployeeSalaryDeserializer();
    }
}