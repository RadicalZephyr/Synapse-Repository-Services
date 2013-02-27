package org.sagebionetworks.repo.model.dbo.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sagebionetworks.repo.model.AuthorizationConstants;
import org.sagebionetworks.repo.model.DatastoreException;
import org.sagebionetworks.repo.model.TrashedEntity;
import org.sagebionetworks.repo.model.UserGroupDAO;
import org.sagebionetworks.repo.model.jdo.KeyFactory;
import org.sagebionetworks.repo.web.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:jdomodels-test-context.xml" })
public class DBOTrashCanDaoImplAutowiredTest {

	@Autowired
	private DBOTrashCanDao trashCanDao;

	@Autowired
	private UserGroupDAO userGroupDAO;

	private String userId;

	@Before
	public void before() throws Exception {

		userId = userGroupDAO.findGroup(AuthorizationConstants.BOOTSTRAP_USER_GROUP_NAME, false).getId();
		assertNotNull(userId);

		clear();

		List<TrashedEntity> trashList = trashCanDao.getInRangeForUser(userId, 0L, Long.MAX_VALUE);
		assertTrue(trashList.size() == 0);
	}

	@After
	public void after() throws Exception {
		clear();
		List<TrashedEntity> trashList = trashCanDao.getInRangeForUser(userId, 0L, Long.MAX_VALUE);
		assertTrue(trashList.size() == 0);
	}

	@Test
	public void testRoundTrip() throws DatastoreException, NotFoundException {

		final String nodeName = "DBOTrashCanDaoImplAutowiredTest.testRoundTrip()";
		final String nodeId1 = KeyFactory.keyToString(555L);
		final String parentId1 = KeyFactory.keyToString(5L);
		List<TrashedEntity> trashList = trashCanDao.getInRangeForUser(userId, 0L, 100L);
		assertNotNull(trashList);
		assertEquals(0, trashList.size());
		TrashedEntity trash = trashCanDao.getTrashedEntity(userId, nodeId1);
		assertNull(trash);

		trashCanDao.create(userId, nodeId1, nodeName, parentId1);
		trashList = trashCanDao.getInRangeForUser(userId, 0L, 100L);
		assertNotNull(trashList);
		assertEquals(1, trashList.size());
		trash = trashList.get(0);
		assertEquals(nodeId1, trash.getEntityId());
		assertEquals(nodeName, trash.getEntityName());
		assertEquals(userId, trash.getDeletedByPrincipalId());
		assertEquals(parentId1, trash.getOriginalParentId());
		assertNotNull(trash.getDeletedOn());

		trash = trashCanDao.getTrashedEntity(userId, nodeId1);
		assertEquals(nodeId1, trash.getEntityId());
		assertEquals(nodeName, trash.getEntityName());
		assertEquals(userId, trash.getDeletedByPrincipalId());
		assertEquals(parentId1, trash.getOriginalParentId());
		assertNotNull(trash.getDeletedOn());

		int count = trashCanDao.getCount(userId);
		assertEquals(1, count);
		count = trashCanDao.getCount(KeyFactory.keyToString(837948837783838309L)); //a random, non-existing user
		assertEquals(0, count);
		boolean exists = trashCanDao.exists(userId, nodeId1);
		assertTrue(exists);
		exists = trashCanDao.exists(KeyFactory.keyToString(2839238478539L), nodeId1);
		assertFalse(exists);
		exists = trashCanDao.exists(userId, KeyFactory.keyToString(118493838393848L));
		assertFalse(exists);

		final String nodeName2 = "DBOTrashCanDaoImplAutowiredTest.testRoundTrip() 2";
		final String nodeId2 = KeyFactory.keyToString(666L);
		final String parentId2 = KeyFactory.keyToString(6L);
		trashCanDao.create(userId, nodeId2, nodeName2, parentId2);
		trashList = trashCanDao.getInRangeForUser(userId, 0L, 100L);
		assertNotNull(trashList);
		assertEquals(2, trashList.size());
		count = trashCanDao.getCount(userId);
		assertEquals(2, count);
		exists = trashCanDao.exists(userId, nodeId2);
		assertTrue(exists);

		trashCanDao.delete(userId, nodeId1);
		trashList = trashCanDao.getInRangeForUser(userId, 0L, 100L);
		assertNotNull(trashList);
		assertEquals(1, trashList.size());
		trash = trashCanDao.getTrashedEntity(userId, nodeId1);
		assertNull(trash);
		trash = trashList.get(0);
		assertEquals(nodeId2, trash.getEntityId());
		assertEquals(nodeName2, trash.getEntityName());
		assertEquals(userId, trash.getDeletedByPrincipalId());
		assertEquals(parentId2, trash.getOriginalParentId());
		assertNotNull(trash.getDeletedOn());
		trash = trashCanDao.getTrashedEntity(userId, nodeId2);
		assertEquals(nodeId2, trash.getEntityId());
		assertEquals(nodeName2, trash.getEntityName());
		assertEquals(userId, trash.getDeletedByPrincipalId());
		assertEquals(parentId2, trash.getOriginalParentId());
		assertNotNull(trash.getDeletedOn());
		count = trashCanDao.getCount(userId);
		assertEquals(1, count);
		exists = trashCanDao.exists(userId, nodeId2);
		assertTrue(exists);
		exists = trashCanDao.exists(userId, nodeId1);
		assertFalse(exists);

		trashCanDao.delete(userId, nodeId2);
		trashList = trashCanDao.getInRangeForUser(userId, 0L, 100L);
		assertNotNull(trashList);
		assertEquals(0, trashList.size());
		count = trashCanDao.getCount(userId);
		assertEquals(0, count);
		exists = trashCanDao.exists(userId, nodeId1);
		assertFalse(exists);
		exists = trashCanDao.exists(userId, nodeId2);
		assertFalse(exists);
		trash = trashCanDao.getTrashedEntity(userId, nodeId2);
		assertNull(trash);
	}

	private void clear() throws Exception {
		List<TrashedEntity> trashList = trashCanDao.getInRangeForUser(userId, 0L, Long.MAX_VALUE);
		for (TrashedEntity trash : trashList) {
			trashCanDao.delete(userId, trash.getEntityId());
		}
	}
}