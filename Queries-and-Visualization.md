#### Queries and Visualization

###### Index

- [``Number of H-1B applications filed through 2008 to 2017``](https://github.com/Kan1shka9/H1B-Data-Analysis-using-Hive-on-Hadoop-Cluster/blob/master/Queries-and-Visualization.md#number-of-h1b-applications-filed-through-2008-to-2017)
- [``Top 10 employers to file H-1B``](https://github.com/Kan1shka9/H1B-Data-Analysis-using-Hive-on-Hadoop-Cluster/blob/master/Queries-and-Visualization.md#top-10-employers-to-file-h1b)
- [``Top 10 employers to file PERM``](https://github.com/Kan1shka9/H1B-Data-Analysis-using-Hive-on-Hadoop-Cluster/blob/master/Queries-and-Visualization.md#top-10-employers-to-file-green-card-perm)
- [``Top companies who filed green card application in year 2017 for F-1 students``](https://github.com/Kan1shka9/H1B-Data-Analysis-using-Hive-on-Hadoop-Cluster/blob/master/Queries-and-Visualization.md#top-companies-who-filed-green-card-application-in-year-2017-for-f-1-students)
- [``Top job titles with highest application over the year 2008-2017``](https://github.com/Kan1shka9/H1B-Data-Analysis-using-Hive-on-Hadoop-Cluster/blob/master/Queries-and-Visualization.md#top-job-titles-with-highest-application-over-the-year-2008-2017)
- [``Average Salary for an analysts position from year 2011 to 2017``](https://github.com/Kan1shka9/H1B-Data-Analysis-using-Hive-on-Hadoop-Cluster/blob/master/Queries-and-Visualization.md#average-salary-for-an-analysts-position-from-year-2011-to-2017)
- [``Highest Average Salaries Based on Job Titles``](https://github.com/Kan1shka9/H1B-Data-Analysis-using-Hive-on-Hadoop-Cluster/blob/master/Queries-and-Visualization.md#highest-average-salaries-based-on-job-titles)
- [``Highest Average Salaries Based on Worksite State``](https://github.com/Kan1shka9/H1B-Data-Analysis-using-Hive-on-Hadoop-Cluster/blob/master/Queries-and-Visualization.md#highest-average-salaries-based-on-worksite-state)

###### Number of H1B applications filed through 2008 to 2017

- Hive Query

 ```sql
 SELECT year, count(*)  FROM h1b_data GROUP BY year ORDER BY year;
 ```
 
- Visualization

![](images/1.png)

###### Top 10 Employers to file H1B

- Hive Query

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS h1b_data
(case_number STRING, 
case_status STRING, 
visa_class STRING,
employer_name STRING,
employer_state STRING,
job_code STRING,
job_title STRING,
prevailing_wage BIGINT,
wage_unit STRING,
worksite_state STRING,
financial_year STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"") 
STORED AS TEXTFILE TBLPROPERTIES("skip.header.line.count"="1");
```

```sql
LOAD DATA INPATH 'example3/H-1B_Data.csv' OVERWRITE INTO TABLE h1b_data;
```

```sql
SELECT employer_name, visa_class, count(employer_name)as count FROM h1b_data WHERE visa_class = H-1B GROUP BY employer_name ORDER BY count desc limit 10;
```

- Result

```
INFOSYS LIMITED	151180
TATA CONSULTANCY SERVICES LIMITED	81147
WIPRO LIMITED	56576
DELOITTE CONSULTING LLP	47368
IBM INDIA PRIVATE LIMITED	45043
ACCENTURE LLP	40492
MICROSOFT CORPORATION	39664
HCL AMERICA, INC.	27997
CAPGEMINI AMERICA INC	26331
ERNST & YOUNG U.S. LLP	24328
```

- Visualization

![](images/2.png)

###### Top 10 Employers to file Green Card (PERM)

- Hive Query

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS perm
(case_number STRING, 
case_status STRING,
employer_name STRING, 
employer_state STRING, 
job_code STRING, 
job_title STRING, 
year INT) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"") 
STORED AS TEXTFILE TBLPROPERTIES("skip.header.line.count"="1");
```

```sql
LOAD DATA INPATH 'example/PERM_Data_Combined.csv' OVERWRITE INTO TABLE perm;
```

```sql
SELECT employer_name, count(employer_name)as count FROM perm GROUP BY employer_name ORDER BY count desc limit 10;
```

- Result

```
COGNIZANT TECHNOLOGY SOLUTIONS US CORPORATION 20619
MICROSOFT CORPORATION	18282
INTEL CORPORATION	8951
GOOGLE INC.	7664
AMAZON CORPORATE LLC	5488
CISCO SYSTEMS, INC.	4783
APPLE INC.	3874
INFOSYS LTD.	3368
ORACLE AMERICA, INC.	3207
DELOITTE CONSULTING LLP	2616
```

- Visualization

![](images/3.png)

###### Top companies who filed green card application in year 2017 for F-1 students

- Hive Query

```sql
create table perm_data2
(Emp_name string,
Class_of_Admission string,
Education string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE TBLPROPERTIES("skip.header.line.count"="1");
```

```sql
LOAD DATA INPATH '/tmp/PERM1.csv' OVERWRITE INTO TABLE perm_data2;
```

```sql
select Emp_name, Class_of_Admission, Education from perm_data2 where Class_of_Admission = 'F-1';
```

- Result

```
INTEL CORPORATION  	F-1     	Master's          	
INTEL CORPORATION  	F-1     	Master's          	
INTEL CORPORATION  	F-1     	Master's          	
INTEL CORPORATION  	F-1     	Master's          	
INTEL CORPORATION  	F-1     	Master's          	
INTEL CORPORATION  	F-1     	Master's          	
INTEL CORPORATION  	F-1     	Master's          	
ilexlaw pllc      	F-1     	Master's          	
Gilsanz Murray Steficek LLP Engineers and Architecture       	F-1     	Doctorate       	
JGF NY Corp    	F-1     	None 	
HARVARD MANAGEMENT ASSOCIATES INC.     	F-1     	Master's          	
Pats Pizzeria Hockessin LLC      	F-1     	None 	
OVERSEAS MANPOWER SOLUTIONS CORP.       	F-1     	None 	
NORA MANUFACTURING INC. 	F-1     	Associate's      	
INTEL CORPORATION  	F-1     	Master's          	
INTEL CORPORATION  	F-1     	Master's          	
INTEL CORPORATION  	F-1     	Master's  
```

- Visualization

![](images/4.png)

###### Top job titles with highest application over the year 2008-2017

- Hive Query

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS h1b_data
(case_number STRING, 
case_status STRING, 
visa_class STRING,
employer_name STRING,
employer_state STRING,
job_code STRING,
job_title STRING,
prevailing_wage BIGINT,
wage_unit STRING,
worksite_state STRING,
financial_year STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"") 
STORED AS TEXTFILE TBLPROPERTIES("skip.header.line.count"="1");
```

```sql
LOAD DATA INPATH "/user/rtiwari2/project/h1b_data.csv" OVERWRITE INTO TABLE h1b_data;
```

```sql
SELECT financial_year, job_code, cnt 
FROM 	
(SELECT financial_year, job_code, count(*) as cnt, RANK() 
OVER (PARTITION BY financial_year ORDER BY count(*) DESC) as rnk 
FROM h1b_data GROUP BY financial_year, job_code) as tg 
WHERE rnk=1;
```

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS yearly_data
(financial_year STRING, job_code STRING, no_of_application BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/rtiwari2/project/';
```

```sql
INSERT OVERWRITE TABLE yearly_data 
SELECT financial_year, job_code, cnt 
FROM 	
(SELECT financial_year, job_code, count(*) as cnt, RANK() 
OVER (PARTITION BY financial_year ORDER BY count(*) DESC) as rnk 
FROM h1b_data GROUP BY financial_year, job_code) as tg 
WHERE rnk=1;
```

- Visualization

![](images/5.png)

###### Average Salary for an analysts position from year 2011 to 2017

- Hive Query

```sql
DROP TABLE IF EXISTS pw_2010_17;
```

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS pw_2010_17
(case_no string, 
visa_class string, 
emp_name string, 
emp_city string, 
emp_state string, 
job_title string, 
education_level string, 
wage_rate double, 
wage_unit string, 
wage_level string, 
year date)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"") 
STORED AS TEXTFILE TBLPROPERTIES("skip.header.line.count"="1");
```

```sql
LOAD DATA INPATH "/user/nikeeta/new/PW_FY2010_2017.csv" OVERWRITE INTO TABLE pw_2010_17;
```

```sql
SELECT  B.YEAR, AVG(B.wage_rate) AS AVG_SALARY FROM pw_2010_17 B
WHERE B.JOB_TITLE LIKE '%Analysts' AND B.VISA_CLASS = 'PERM' 
GROUP BY B.year 
ORDER BY B.year LIMIT 7;
```

- Visualization

![](images/8.png)

###### Highest Average Salaries Based on Job Titles 

- Hive Query

```sql
DROP TABLE IF EXISTS h1b; 
CREATE EXTERNAL TABLE IF NOT EXISTS h1b
(case_status string,
visa_class string, 
employer_name string,
job_title string, 
salary double, 
worksite_city string,
worksite_state string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"") 
STORED AS TEXTFILE TBLPROPERTIES("skip.header.line.count"="1");
```

```sql
LOAD DATA INPATH "/user/yuvasree/analysis/h1bdata.csv" OVERWRITE INTO TABLE h1b;
```

```sql
SELECT B.JOB_TITLE, AVG(B.salary) AS AVG_SALARY FROM H1B B
WHERE B.JOB_TITLE = 'ANALYST' OR B.JOB_TITLE ='DATA SCIENTIST' OR B.JOB_TITLE ='DATA ENGINEER' OR B.JOB_TITLE ='CONSULTANT' AND VISA_CLASS = 'H-1B' AND 
CASE_STATUS = 'CERTIFIED' GROUP BY B.JOB_TITLE
ORDER BY AVG_SALARY DESC 
```

- Result

```
DATA ENGINEER	 92965.55011441647 → 92K
DATA SCIENTIST 87162.42280487805 → 87K
CONSULTANT 70405.68856489423 → 70K 
ANALYST 64469.57977812995 → 65K
```

- Visualization

![](images/6.png)

###### Highest Average Salaries Based on Worksite State

- Hive Query

```sql
DROP TABLE IF EXISTS h1b; 
CREATE EXTERNAL TABLE IF NOT EXISTS h1b
(case_status string,
visa_class string, 
employer_name string,
job_title string, 
salary double, 
worksite_city string,
worksite_state string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"") 
STORED AS TEXTFILE TBLPROPERTIES("skip.header.line.count"="1");
```

```sql
LOAD DATA INPATH "/user/yuvasree/analysis/h1bdata.csv" OVERWRITE INTO TABLE h1b;
```

```sql
SELECT B.WORKSITE_STATE, AVG(B.salary) AS AVG_SALARY FROM H1B B
WHERE
VISA_CLASS = 'H-1B' AND 
CASE_STATUS = 'CERTIFIED'
GROUP BY B.WORKSITE_STATE
ORDER BY AVG_SALARY DESC LIMIT 10
```

- Result

```
WV: West Virginia 94158.85
CA: California 92200.0002853562
WA: Washington 90263.96657461085
WY: Wyoming 80036.92800000001
OR: Oregon 78575.1987223465
NY: New York 78446.32727248245
MA: Massachusetts 78209.25116780466
DC: Washington DC 78082.35710863986
CT: Connecticut 78050.97998304127
MT:Montana 77900.45751633987
```

- Visualization

![](images/7.png)

[![](images/thumb.png)](https://www.youtube.com/watch?v=iy3yLLoTTfg?rel=0 "Highest Average Salaries Based on Worksite State")
