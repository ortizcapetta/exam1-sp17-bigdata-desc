
create table student(region STRING, district STRING, scid INT, sname STRING, level STRING, sex STRING, sid INT);
create table school(region STRING, district STRING, city STRING, sid INT, sname STRING, level STRING, series INT);



-- Find the total number of students per region and city

select student.region, school.city,count(*) from student,school where school.region = student.region group by student.region, school.city; 

--Find the total number of schools per city and level

select school.city, school.level,count(*) from school group by school.city, school.level;

--Find total num of female students from Ponce that go a 'Superior' level school.

select count(*) from school,student where school.sid = scid and school.level ='Superior' and student.sex = 'F' and school.city = 'Ponce';

--Find the total number of male students per region,district and city

select school.region,school.district,school.city, count(*)  from school,student where school.sid = student.scid and student.sex = 'M' group by school.region,school.district,school.city;



