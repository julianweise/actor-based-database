package de.hpi.julianweise.domain.custom.sfemployeesalary;

import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SFEmployeeSalary extends ADBEntity {

    public String yearType;
    public int year;
    public int organizationGroupCode;
    public String organizationGroup;
    public String departmentCode;
    public String department;
    public int unionCode;
    public String union;
    public String jobFamilyCode;
    public String jobFamily;
    public String jobCode;
    public String job;
    public int employeeIdentifier;
    public double salaries;
    public double overtime;
    public double otherSalaries;
    public double totalSalary;
    public double retirement;
    public double healthAndDental;
    public double otherBenefits;
    public double totalBenefits;
    public double totalCompensation;

    @Override
    public ADBKey getPrimaryKey() {
        return new ADBIntegerKey(this.employeeIdentifier);
    }

    @Override
    public int getSize() {
        return 9 * Double.BYTES + 4 * Integer.BYTES
                + this.calculateStringMemoryFootprint(this.yearType.length())
                + this.calculateStringMemoryFootprint(this.organizationGroup.length())
                + this.calculateStringMemoryFootprint(this.departmentCode.length())
                + this.calculateStringMemoryFootprint(this.department.length())
                + this.calculateStringMemoryFootprint(this.union.length())
                + this.calculateStringMemoryFootprint(this.jobFamilyCode.length())
                + this.calculateStringMemoryFootprint(this.jobFamily.length())
                + this.calculateStringMemoryFootprint(this.jobCode.length())
                + this.calculateStringMemoryFootprint(this.job.length());
    }

    @Override
    public String toString() {
        return "[EmployeesSalary] for  Employee#" + this.employeeIdentifier + " in " + this.year;
    }

}
