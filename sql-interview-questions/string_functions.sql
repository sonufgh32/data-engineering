
SELECT CONCAT('Sonu ', 'Monu') FROM DUAL;

SELECT LENGTH('Shiv Ram') FROM DUAL;

SELECT
    UPPER('Shiv Ram'),
    LOWER('Shiv Ram'),
    TRIM('     Shiv Ram     '),
    LTRIM('     Shiv Ram     '),
    RTRIM('     Shiv Ram     ')
FROM
    DUAL;

SELECT REPLACE('Hello World', 'World', 'Universe') FROM DUAL;

SELECT SUBSTR('Hello World', 1, 1) FROM DUAL;

SELECT
    -- NVL(NULL, 'Sonu', NULL),
    NVL('Sonu', null),
    NVL(NULL, 'Sonu')
FROM
    DUAL;

SELECT NVL2(1, 'SALARY', 'NO SALARY') FROM DUAL;
SELECT NVL2(NULL, 'SALARY', 'NO SALARY') FROM DUAL;

SELECT COALESCE(1, NULL, 3, 4, 5) FROM DUAL;
SELECT COALESCE(1, 2, NULL, 4, 5) FROM DUAL;
SELECT COALESCE(NULL, 2, 3, 4, 5) FROM DUAL;