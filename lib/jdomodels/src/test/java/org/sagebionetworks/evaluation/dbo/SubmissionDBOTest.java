package org.sagebionetworks.evaluation.dbo;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sagebionetworks.evaluation.dbo.EvaluationDBO;
import org.sagebionetworks.evaluation.dbo.ParticipantDBO;
import org.sagebionetworks.evaluation.dbo.SubmissionDBO;
import org.sagebionetworks.evaluation.model.EvaluationStatus;
import org.sagebionetworks.repo.model.DatastoreException;
import org.sagebionetworks.repo.model.InvalidModelException;
import org.sagebionetworks.repo.model.Node;
import org.sagebionetworks.repo.model.NodeDAO;
import org.sagebionetworks.repo.model.dbo.DBOBasicDao;
import org.sagebionetworks.repo.model.jdo.NodeTestUtils;
import org.sagebionetworks.repo.web.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:jdomodels-test-context.xml" })
public class SubmissionDBOTest {
 
    @Autowired
    DBOBasicDao dboBasicDao;
	@Autowired
	NodeDAO nodeDAO;
 
    private String nodeId = null;
    private long submissionId = 2000;
    private long userId = 0;
    private long evalId;
    private String name = "test submission";
    
    @Before
    public void setUp() throws DatastoreException, InvalidModelException, NotFoundException {    	
    	// create a node
  		Node toCreate = NodeTestUtils.createNew(name, userId);
    	toCreate.setVersionComment("This is the first version of the first node ever!");
    	toCreate.setVersionLabel("0.0.1");
    	nodeId = nodeDAO.createNew(toCreate).substring(3); // trim "syn" from node ID
    	
        // Initialize a new Evaluation
        EvaluationDBO evaluation = new EvaluationDBO();
        evaluation.setId(evalId);
        evaluation.seteTag("etag");
        evaluation.setName("name");
        evaluation.setOwnerId(userId);
        evaluation.setContentSource("foobar");
        evaluation.setCreatedOn(System.currentTimeMillis());
        evaluation.setStatusEnum(EvaluationStatus.PLANNED);
        evalId = dboBasicDao.createNew(evaluation).getId();
        
        // Initialize a new Participant
        ParticipantDBO participant = new ParticipantDBO();
        participant.setUserId(userId);
        participant.setEvalId(evalId);
        participant.setCreatedOn(System.currentTimeMillis());
        dboBasicDao.createNew(participant);
    }
    
    @After
    public void after() throws DatastoreException {
        if(dboBasicDao != null) {
        	// delete submission
            MapSqlParameterSource params = new MapSqlParameterSource();
            params.addValue("id", submissionId);
            dboBasicDao.deleteObjectById(SubmissionDBO.class, params);
            
            // delete participant
            params = new MapSqlParameterSource();
            params.addValue("userId", userId);
            params.addValue("evalId", evalId);
            dboBasicDao.deleteObjectById(ParticipantDBO.class, params);
            
            // delete Evaluation
            params = new MapSqlParameterSource();
            params.addValue("id", evalId);
            dboBasicDao.deleteObjectById(EvaluationDBO.class, params);
        }
    	try {
    		nodeDAO.delete(nodeId);
    	} catch (NotFoundException e) {};
    }
    @Test
    public void testCRD() throws Exception{
        // Initialize a new Submission
        SubmissionDBO submission = new SubmissionDBO();
        submission.setId(submissionId);
        submission.setName(name);
        submission.setEntityId(Long.parseLong(nodeId));
        submission.setVersionNumber(1L);
        submission.setUserId(userId);
        submission.setEvalId(evalId);
        submission.setCreatedOn(System.currentTimeMillis());
 
        // Create it
        SubmissionDBO clone = dboBasicDao.createNew(submission);
        assertNotNull(clone);
        assertEquals(submission, clone);
        
        // Fetch it
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("id",submissionId);
        SubmissionDBO clone2 = dboBasicDao.getObjectById(SubmissionDBO.class, params);
        assertNotNull(clone2);
        assertEquals(submission, clone2); 
        
        // Delete it
        boolean result = dboBasicDao.deleteObjectById(SubmissionDBO.class,  params);
        assertTrue("Failed to delete the entry created", result); 
    }
 
}