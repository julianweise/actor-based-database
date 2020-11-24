SELECT a.id AS a_id, b.id AS b_id
FROM SFEmployeeSalary a, SFEmployeeSalary b
WHERE a.jobCode = b.jobCode
AND a.JobFamilyCode != b.JobFamilyCode;
