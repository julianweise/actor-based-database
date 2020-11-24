SELECT a.id AS a_id, b.id AS b_id
FROM SFEmployeeSalary a, SFEmployeeSalary b
WHERE a.totalBenefits > b.totalBenefits
AND a.totalSalary > b.totalSalary
AND a.totalCompensation <= b.totalCompensation;
