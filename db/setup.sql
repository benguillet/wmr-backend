CREATE TABLE IF NOT EXISTS Submissions
(
ID           IDENTITY,
User         VARCHAR(50),
Name         VARCHAR(50),
Timestamp    TIMESTAMP DEFAULT NOW(),
Test         BOOLEAN,
Submitted    BOOLEAN DEFAULT FALSE,
Completed    BOOLEAN DEFAULT FALSE,
Input        VARCHAR(255),

-- Hadoop jobs only
Hadoop_ID    VARCHAR(50),

-- Test jobs only
Mapper       VARCHAR(255),
Reducer      VARCHAR(255)
);
