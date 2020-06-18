package de.hpi.julianweise.domain.custom.sfemployeesalary;

import com.univocity.parsers.common.record.Record;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBEntityFactory;

public class SFEmployeeFactory implements ADBEntityFactory {

    @Override
    public Class<? extends ADBEntity> getTargetClass() {
        return SFEmployeeSalary.class;
    }

    @Override
    public ADBEntity build(Record record) {
        return SFEmployeeSalary.builder()
                               .yearType(record.getString(0))
                               .year(record.getInt(1))
                               .organizationGroupCode(record.getInt(2))
                               .organizationGroup(record.getString(3))
                               .departmentCode(record.getString(4))
                               .department(record.getString(5))
                               .unionCode(record.getString(6))
                               .union(record.getString(7))
                               .jobFamilyCode(record.getString(8))
                               .jobFamily(record.getString(9))
                               .jobCode(record.getString(10))
                               .job(record.getString(11))
                               .employeeIdentifier(record.getInt(12))
                               .salaries(record.getDouble(13))
                               .overtime(record.getDouble(14))
                               .otherSalaries(record.getDouble(15))
                               .totalSalary(record.getDouble(16))
                               .retirement(record.getDouble(17))
                               .healthAndDental(record.getDouble(18))
                               .otherBenefits(record.getDouble(19))
                               .totalBenefits(record.getDouble(20))
                               .totalCompensation(record.getDouble(21))
                               .build();
    }

    @Override
    public SFEmployeeSalaryDeserializer buildDeserializer() {
        return new SFEmployeeSalaryDeserializer();
    }
}