CREATE KEYSPACE nhom12 WITH REPLICATION = {
    'class': 'SimpleStrategy', 
    'replication_factor': 1
};

CREATE TABLE nhom12.Department (
    IDDepartment text PRIMARY KEY, 
    NameDepartment text
);

CREATE TABLE nhom12.Class (
    IDClass text PRIMARY KEY, 
    NameClass text, 
    IDDepartment text, 
    NameDepartment text
);

CREATE TABLE nhom12.Student (
    IDStudent text PRIMARY KEY,
    NameStudent text, 
    PhoneNumber text, 
    Address text, 
    IDClass text, 
    NameClass text,
    IDDepartment text,
    NameDepartment text
);

CREATE TABLE nhom12.Subject (
    IDSubject text PRIMARY KEY,
    NameSubject text, 
    Credit int
);

CREATE TABLE nhom12.Grade (
    IDStudent text, 
    NameStudent text, 
    IDSubject text, 
    NameSubject text, 
    Grade float, 
    PRIMARY KEY (IDStudent, IDSubject)
);
