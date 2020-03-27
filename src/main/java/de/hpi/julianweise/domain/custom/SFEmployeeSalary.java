package de.hpi.julianweise.domain.custom;

import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.domain.key.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class SFEmployeeSalary extends ADBEntityType {

    private String yearType;
    private int year;
    private int organizationGroupCode;
    private String departmentCode;
    private String unionCode;
    private String jobFamilyCode;
    private String jobCode;
    private int employeeIdentifier;
    private double salaries;
    private double overtime;
    private double otherSalaries;
    private double totalSalary;
    private double retirement;
    private double health;
    private double otherBenefits;
    private double totalBenefits;
    private double totalCompensation;

    @Override
    public ADBKey getPrimaryKey() {
        return new ADBIntegerKey(this.employeeIdentifier);
    }

    @Override
    public String toString() {
        return "[EmployeesSalary] for  Employee#" + this.employeeIdentifier + " in " + this.year;
    }

}
