SELECT a.id AS a_id, b.id AS b_id
FROM SFEmployeeSalary a, SFEmployeeSalary b
WHERE a.salaries = b.salaries
AND a.overtime = b.overtime
AND a.otherSalaries = b.otherSalaries
AND a.totalSalary = b.totalSalary;
