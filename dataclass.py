from dataclasses import dataclass

# Định nghĩa các lớp Python bằng dataclass
@dataclass
class Department:
    iddepartment: str
    namedepartment: str

@dataclass
class Classes:
    idclass: str
    nameclass: str
    iddepartment: str

@dataclass
class Student:
    idstudent: str
    namestudent: str
    phonenumber: str
    address: str
    idclass: str

@dataclass
class Subject:
    idsubject: str
    namesubject: str
    credit: int

@dataclass
class Grade:
    idstudent: str
    idsubject: str
    term: int
    grade: float