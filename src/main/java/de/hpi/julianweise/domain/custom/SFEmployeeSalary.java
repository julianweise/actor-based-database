package de.hpi.julianweise.domain.custom;

import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.domain.key.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder
@AllArgsConstructor
@Getter
public class SFEmployeeSalary extends ADBEntityType {

    private final String yearType;
    private final int year;
    private final int organizationGroupCode;
    private final String departmentCode;
    private final String unionCode;
    private final String jobFamilyCode;
    private final String jobCode;
    private final int employeeIdentifier;
    private final double salaries;
    private final double overtime;
    private final double otherSalaries;
    private final double totalSalary;
    private final double retirement;
    private final double health;
    private final double otherBenefits;
    private final double totalBenefits;
    private final double totalCompensation;

    @Override
    public ADBKey getPrimaryKey() {
        return new ADBIntegerKey(this.employeeIdentifier);
    }

    @Override
    public String toString() {
        return "[EmployeesSalary] for  Employee#" + this.employeeIdentifier + " in " + this.year;
    }

}
