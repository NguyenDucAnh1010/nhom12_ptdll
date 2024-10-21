CREATE KEYSPACE nhom12 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE nhom12;

CREATE TABLE nhom12.Department (
    IDDepartment text PRIMARY KEY, 
    NameDepartment text
);

CREATE TABLE nhom12.Class (
    IDClass text PRIMARY KEY, 
    NameClass text, 
    IDDepartment text
);

CREATE TABLE nhom12.Student (
    IDStudent text PRIMARY KEY,
    NameStudent text,
    PhoneNumber text, 
    Address text, 
    IDClass text, 
);

CREATE TABLE nhom12.Subject (
    IDSubject text PRIMARY KEY,
    NameSubject text, 
    Credit int
);

CREATE TABLE nhom12.Grade (
    IDStudent text, 
    IDSubject text, 
    Grade float, 
    PRIMARY KEY (IDStudent, IDSubject)
);
