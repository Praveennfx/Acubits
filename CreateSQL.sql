CREATE SCHEMA TestSchema;
Go

CREATE TABLE TestSchema.Course (
  Search NVARCHAR(150),
  Name NVARCHAR(200),
  Description NVARCHAR(4000),
);
Go

CREATE TABLE TestSchema.Author (
  Name NVARCHAR(200),
  Author NVARCHAR(200),
);
Go