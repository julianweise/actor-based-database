package de.hpi.julianweise.domain.custom;

import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.domain.key.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Builder
@AllArgsConstructor
public class SFEmployeeSalary extends ADBEntityType {

    public final String yearType;
    public final int year;
    public final int organizationGroupCode;
    public final String departmentCode;
    public final String unionCode;
    public final String jobFamilyCode;
    public final String jobCode;
    public final int employeeIdentifier;
    public final double salaries;
    public final double overtime;
    public final double otherSalaries;
    public final double totalSalary;
    public final double retirement;
    public final double health;
    public final double otherBenefits;
    public final double totalBenefits;
    public final double totalCompensation;

    @Override
    public ADBKey getPrimaryKey() {
        return new ADBIntegerKey(this.employeeIdentifier);
    }

    @Override
    public String toString() {
        return "[EmployeesSalary] for  Employee#" + this.employeeIdentifier + " in " + this.year;
    }

}
