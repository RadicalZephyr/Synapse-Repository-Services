CREATE TABLE JDOPARTICIPANT (
    ID bigint(20) NOT NULL,
    COMPETITION_ID bigint(20) NOT NULL,
    JOIN_DATE bigint(20) NOT NULL,
    SCORE bigint(20) DEFAULT NULL,
    PRIMARY KEY (ID),
    UNIQUE KEY UNIQUE_PARTICIPANT_COMPETITION_TUPLE (ID , COMPETITION_ID),
    CONSTRAINT COMPETITION_ID_FK FOREIGN KEY (COMPETITION_ID) REFERENCES JDOCOMPETITION (ID) ON DELETE CASCADE,
    CONSTRAINT `PARTICIPANT_ID_FK` FOREIGN KEY (`ID`) REFERENCES `JDOUSERGROUP` (`ID`) ON DELETE CASCADE
);