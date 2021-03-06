package org.sagebionetworks.repo.model.dbo.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sagebionetworks.repo.model.ObjectType;
import org.sagebionetworks.repo.model.ProcessedMessageDAO;
import org.sagebionetworks.repo.model.message.ChangeMessage;
import org.sagebionetworks.repo.model.message.ChangeMessageUtils;
import org.sagebionetworks.repo.model.message.ChangeType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:jdomodels-test-context.xml" })
public class DBOChangeDAOImplAutowiredTest {
	
	@Autowired
	DBOChangeDAO changeDAO;
	
	@Autowired
	ProcessedMessageDAO processedMessageDAO;
	
	@Before
	public void before(){
		if(changeDAO != null){
			changeDAO.deleteAllChanges();
		}
	}
	
	@After
	public void after(){
		if(changeDAO != null){
			changeDAO.deleteAllChanges();
		}
	}
	
	@Test
	public void testGetCurrentChangeNumberEmpty() {
		long ccn = changeDAO.getCurrentChangeNumber();
		assertEquals(0L, ccn);
	}
	
	@Test
	public void testGetMinimumChangeNumberEmpty() {
		long mcn = changeDAO.getMinimumChangeNumber();
		assertEquals(0L, mcn);
	}
	
	@Test
	public void testGetCountEmpty() {
		long count = changeDAO.getCount();
		assertEquals(0L, count);
	}
	
	@Test
	public void testReplace(){
		ChangeMessage change = new ChangeMessage();
		change.setObjectId("syn123");
		change.setObjectEtag("myEtag");
		change.setChangeType(ChangeType.CREATE);
		change.setObjectType(ObjectType.ENTITY);
		ChangeMessage clone = changeDAO.replaceChange(change);
		assertNotNull(clone);
		System.out.println(clone);
		assertNotNull(clone.getChangeNumber());
		assertNotNull(clone.getTimestamp());
		long firstChangeNumber = clone.getChangeNumber();
		assertEquals(change.getObjectId(), clone.getObjectId());
		assertEquals(change.getObjectEtag(), clone.getObjectEtag());
		assertEquals(change.getChangeType(), clone.getChangeType());
		assertEquals(change.getObjectType(), clone.getObjectType());
		// Now replace it again
		clone = changeDAO.replaceChange(change);
		assertNotNull(clone);
		long secondChangeNumber = clone.getChangeNumber();
		System.out.println(clone);
		assertTrue(secondChangeNumber > firstChangeNumber);
	}
	
	/**
	 * ObjectIds can be duplicated, so make sure replace uses a composite key.
	 */
	@Test
	public void testReplaceDuplicateObjectId(){
		ChangeMessage changeOne = new ChangeMessage();
		changeOne.setObjectId("123");
		changeOne.setObjectEtag("myEtag");
		changeOne.setChangeType(ChangeType.CREATE);
		changeOne.setObjectType(ObjectType.ACTIVITY);
		changeDAO.replaceChange(changeOne);
		
		// Now create a second change with the same id but different type.
		ChangeMessage changeTwo = new ChangeMessage();
		changeTwo.setObjectId(changeOne.getObjectId());
		changeTwo.setObjectEtag("myEtag");
		changeTwo.setChangeType(ChangeType.CREATE);
		changeTwo.setObjectType(ObjectType.PRINCIPAL);
		changeDAO.replaceChange(changeTwo);
		
		// Now we should see both changes listed
		List<ChangeMessage> list = changeDAO.listChanges(0, ObjectType.ACTIVITY, 100);
		assertNotNull(list);
		assertEquals(1, list.size());
		ChangeMessage message =  list.get(0);
		assertEquals(ObjectType.ACTIVITY, message.getObjectType());
		// Check the principal list
		list = changeDAO.listChanges(0, ObjectType.PRINCIPAL, 100);
		assertNotNull(list);
		assertEquals(1, list.size());
		message =  list.get(0);
		assertEquals(ObjectType.PRINCIPAL, message.getObjectType());
		
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void tesNullId(){
		ChangeMessage change = new ChangeMessage();
		change.setObjectId(null);
		change.setObjectEtag("myEtag");
		change.setChangeType(ChangeType.CREATE);
		change.setObjectType(ObjectType.ENTITY);
		changeDAO.replaceChange(change);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void tesNullChangeType(){
		ChangeMessage change = new ChangeMessage();
		change.setObjectId("syn334");
		change.setObjectEtag("myEtag");
		change.setChangeType(null);
		change.setObjectType(ObjectType.ENTITY);
		changeDAO.replaceChange(change);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void tesNullObjectTypeType(){
		ChangeMessage change = new ChangeMessage();
		change.setObjectId("syn123");
		change.setObjectEtag("myEtag");
		change.setChangeType(ChangeType.CREATE);
		change.setObjectType(null);
		changeDAO.replaceChange(change);
	}
	
	/**
	 * Etag can be null for deletes.
	 */
	@Test
	public void tesNullEtagForDelete(){
		ChangeMessage change = new ChangeMessage();
		change.setObjectId("syn223");
		change.setObjectEtag(null);
		change.setChangeType(ChangeType.DELETE);
		change.setObjectType(ObjectType.ENTITY);
		ChangeMessage clone = changeDAO.replaceChange(change);
		assertNotNull(clone);
		assertNull(clone.getObjectEtag());
	}
	
	/**
	 * Etag must not be null for create or update.
	 */
	@Test (expected=IllegalArgumentException.class)
	public void tesNullEtagForCreate(){
		ChangeMessage change = new ChangeMessage();
		change.setObjectId("syn334");
		change.setObjectEtag(null);
		change.setChangeType(ChangeType.CREATE);
		change.setObjectType(ObjectType.ENTITY);
		ChangeMessage clone = changeDAO.replaceChange(change);
		assertNotNull(clone);
		assertNull(clone.getObjectEtag());
	}
	
	/**
	 * Etag must not be null for create or update.
	 */
	@Test (expected=IllegalArgumentException.class)
	public void tesNullEtagForUpdate(){
		ChangeMessage change = new ChangeMessage();
		change.setObjectId("syn334");
		change.setObjectEtag(null);
		change.setChangeType(ChangeType.UPDATE);
		change.setObjectType(ObjectType.ENTITY);
		ChangeMessage clone = changeDAO.replaceChange(change);
		assertNotNull(clone);
		assertNull(clone.getObjectEtag());
	}
	
	@Test
	public void testSortByObjectId(){
		List<ChangeMessage> batch = createList(5, ObjectType.PRINCIPAL);
		// Start shuffled
		Collections.shuffle(batch);
		// Now sort
		batch = ChangeMessageUtils.sortByObjectId(batch);
		assertNotNull(batch);
		long previous = -1;
		for(ChangeMessage change: batch){
			Long id = Long.parseLong(change.getObjectId());
			assertTrue(id > previous);
			previous = id;
		}
	}
	
	@Test
	public void testSortByIdSameIdDifferentType(){
		List<ChangeMessage> batch = new LinkedList<ChangeMessage>();
		ChangeMessage message = new ChangeMessage();
		message.setObjectId("123");
		message.setObjectType(ObjectType.PRINCIPAL);
		batch.add(message);
		message = new ChangeMessage();
		message.setObjectId("123");
		message.setObjectType(ObjectType.ACTIVITY);
		batch.add(message);
		// Now sort
		batch = ChangeMessageUtils.sortByObjectId(batch);
		assertNotNull(batch);
		// the activity should be first now
		assertEquals("Activity should have been placed before principal",ObjectType.ACTIVITY, batch.get(0).getObjectType());
		assertEquals("Activity should have been placed before principal",ObjectType.PRINCIPAL, batch.get(1).getObjectType());
	}
	
	@Test
	public void testSortByChangeNumber(){
		List<ChangeMessage> batch = new ArrayList<ChangeMessage>();
		for(int i=0; i<5; i++){
			ChangeMessage change = new ChangeMessage();
			change.setObjectId("syn"+i);
			change.setObjectEtag("etag"+i);
			change.setChangeType(ChangeType.UPDATE);
			change.setObjectType(ObjectType.ENTITY);
			change.setChangeNumber(new Long(i));
			batch.add(change);
		}
		// Start shuffled
		Collections.shuffle(batch);
		// Now sort
		batch = ChangeMessageUtils.sortByChangeNumber(batch);
		assertNotNull(batch);
		long previous = -1;
		for(ChangeMessage change: batch){
			assertTrue(change.getChangeNumber() > previous);
			previous = change.getChangeNumber();
		}
	}
	
	@Test
	public void testReplaceBatch(){
		// Get the current change number
		int numChangesInBatch = 5;
		long startChangeNumber = startChangeNumber();
		List<ChangeMessage> batch = createList(5, ObjectType.PRINCIPAL);
		// We want to start with an unordered list of changes
		// because the batch replace must sort the list by object id
		// to ensure a consistent update order to prevent deadlock.
		Collections.shuffle(batch);
		System.out.println(batch);
		// Pass the batch.
		batch  = changeDAO.replaceChange(batch);
		// The resulting list 
		assertNotNull(batch);
		
		// If the changes were sorted before replaced, then sorting by change number should
		// give use the same order as sorting by objectId.
		// by change number
		List<ChangeMessage> byChangeNumber = new LinkedList<ChangeMessage>(batch);
		byChangeNumber = ChangeMessageUtils.sortByChangeNumber(byChangeNumber);
		// by object id
		List<ChangeMessage> byObjectId = new LinkedList<ChangeMessage>(batch);
		byObjectId = ChangeMessageUtils.sortByObjectId(byObjectId);
		assertEquals("If the batch was sorted by objectID before replacing then the change number should be in the same order as the object ids", byChangeNumber, byObjectId);
		
		// Check the change numbers
		long endChangeNumber = changeDAO.getCurrentChangeNumber();
		assertEquals(startChangeNumber + numChangesInBatch, endChangeNumber);
		
		// The element inserted by startChangeNumber() will be replaced by an element created in createList(5, ...)
		long minChangeNumber = changeDAO.getMinimumChangeNumber();
		assertEquals(startChangeNumber + 1, minChangeNumber);
		
		long countChangeNumber = changeDAO.getCount();
		assertEquals(numChangesInBatch, countChangeNumber);
	}
	
	@Test
	public void testListChangesNullType(){
		// Get the current change number
		List<ChangeMessage> batch = createList(2, ObjectType.ENTITY);
		// Pass the batch.
		batch  = changeDAO.replaceChange(batch);
		// The resulting list 
		assertNotNull(batch);
		// Now listing this should return the same as the batch
		List<ChangeMessage> list = changeDAO.listChanges(batch.get(0).getChangeNumber(), null, 10);
		assertEquals(batch, list);
	}
	
	@Test
	public void testListChangesType(){
		// Get the current change number
		List<ChangeMessage> batch = new ArrayList<ChangeMessage>();
		List<ChangeMessage> expectedFiltered = new ArrayList<ChangeMessage>();
		for(int i=0; i<5; i++){
			ChangeMessage change = new ChangeMessage();
			change.setObjectEtag("etag"+i);
			change.setChangeType(ChangeType.UPDATE);
			if(i%2 > 0){
				change.setObjectType(ObjectType.ENTITY);
				change.setObjectId("syn"+i);
				expectedFiltered.add(change);
			}else{
				change.setObjectType(ObjectType.PRINCIPAL);
				change.setObjectId(""+i);
			}
			batch.add(change);
		}
		// Pass the batch.
		batch  = changeDAO.replaceChange(batch);
		// Now listing this should return the same as the batch
		List<ChangeMessage> list = changeDAO.listChanges(batch.get(0).getChangeNumber(), ObjectType.ENTITY, 10);
		// Clear the timestamps and changeNumber before we do the compare
		for(ChangeMessage cm: list){
			assertNotNull(cm.getTimestamp());
			assertNotNull(cm.getChangeNumber());
			cm.setTimestamp(null);
			cm.setChangeNumber(null);
		}
		assertEquals(expectedFiltered, list);
	}
	
	@Test
	public void testRegisterSentAndListUnsent(){
		// Create a few messages.
		List<ChangeMessage> batch = createList(2, ObjectType.ENTITY);
		// Pass the batch.
		batch  = changeDAO.replaceChange(batch);
		// Both should be listed as unsent.
		List<ChangeMessage> unSent = changeDAO.listUnsentMessages(3); 
		assertEquals(batch, unSent);
		// Now register one
		assertTrue(changeDAO.registerMessageSent(batch.get(1)));
		assertFalse("Registering the same change twice should not result in an update",changeDAO.registerMessageSent(batch.get(1)));
		unSent = changeDAO.listUnsentMessages(3);
		assertNotNull(unSent);
		assertEquals(1, unSent.size());
		assertEquals(batch.get(0).getChangeNumber(), unSent.get(0).getChangeNumber());
		// Register the second
		changeDAO.registerMessageSent(batch.get(0));
		unSent = changeDAO.listUnsentMessages(3);
		assertNotNull(unSent);
		assertEquals(0, unSent.size());
	}
	
	@Test
	public void testReplaceDeleteSent(){
		List<ChangeMessage> batch = createList(1, ObjectType.ENTITY);
		// Pass the batch.
		batch  = changeDAO.replaceChange(batch);
		// Send the batch
		changeDAO.registerMessageSent(batch.get(0));
		// Replace the batch again
		batch  = changeDAO.replaceChange(batch);
		// This will fail if we did not delete the sent message.
		changeDAO.registerMessageSent(batch.get(0));
	}
	
	@Test
	public void testRegisterMessageSent(){
		ChangeMessage change = createList(1, ObjectType.ENTITY).get(0);
		// Pass the batch.
		change  = changeDAO.replaceChange(change);
		// Send the batch
		assertTrue(changeDAO.registerMessageSent(change));
		// Calling it again should return false since it was already sent
		assertFalse("The messages should already be marked as sent", changeDAO.registerMessageSent(change));
		// Now if we replace the change we should be able to register it sent again
		change  = changeDAO.replaceChange(change);
		assertTrue(changeDAO.registerMessageSent(change));
	}
	
	@Test
	public void testRegisterMessageSentOldChange(){
		ChangeMessage change = createList(1, ObjectType.ENTITY).get(0);
		// Pass the batch.
		change  = changeDAO.replaceChange(change);
		// use an older change number
		change.setChangeNumber(change.getChangeNumber()-1);
		// Send the batch
		assertFalse("Since the passed change number doe snot match the current state of the change, the sent registration should not have happened.", changeDAO.registerMessageSent(change));
	}
	
	
	@Test
	public void testGetMaxSentChangeNumber(){
		assertEquals("When the sent messages is empty, the max sent change number should be -1",new Long(-1), changeDAO.getMaxSentChangeNumber(Long.MAX_VALUE));
		List<ChangeMessage> batch = createList(3, ObjectType.ENTITY);
		// Add all three to changes
		batch  = changeDAO.replaceChange(batch);
		// Only add the first and last to sent.
		changeDAO.registerMessageSent(batch.get(0));
		changeDAO.registerMessageSent(batch.get(2));
		Long firstChangeNumber = batch.get(0).getChangeNumber();
		Long secondChangeNumber = batch.get(1).getChangeNumber();
		Long thirdChangeNumber = batch.get(2).getChangeNumber();
		assertEquals(firstChangeNumber, changeDAO.getMaxSentChangeNumber(firstChangeNumber));
		assertEquals("Since the second change number was not sent, the max sent change number less than or equals to the second should be the first.",firstChangeNumber, changeDAO.getMaxSentChangeNumber(secondChangeNumber));
		assertEquals(thirdChangeNumber, changeDAO.getMaxSentChangeNumber(thirdChangeNumber));
		assertEquals(new Long(-1), changeDAO.getMaxSentChangeNumber(new Long(-1)));
	}
	
	@Test
	public void testUpdateSentMessageSameIdDifferentType(){
		// Create a few messages.
		List<ChangeMessage> batch = createList(2, ObjectType.ENTITY);
		ChangeMessage zero = batch.get(0);
		zero.setObjectId("123");
		zero.setObjectType(ObjectType.TABLE);
		ChangeMessage one = batch.get(1);
		one.setObjectId("123");
		one.setObjectType(ObjectType.ENTITY);
		// Pass the batch.
		batch  = changeDAO.replaceChange(batch);
		// Register as sent
		changeDAO.registerMessageSent(batch.get(0));
		changeDAO.registerMessageSent(batch.get(1));
		// Pass the batch.
		batch  = changeDAO.replaceChange(batch);
		// again
		changeDAO.registerMessageSent(batch.get(0));
		changeDAO.registerMessageSent(batch.get(1));
	}
	
	@Test
	public void testRegisterProcessedAndListNotProcessed() throws Exception{
		// Create msgs
		List<ChangeMessage> batch = createList(3, ObjectType.ENTITY);
		batch = changeDAO.replaceChange(batch);
		List<ChangeMessage> notProcessed = processedMessageDAO.listNotProcessedMessages("Q", 3);
		assertEquals(0, notProcessed.size());
		// Register sent msgs
		changeDAO.registerMessageSent(batch.get(0));
		Thread.sleep(500);
		changeDAO.registerMessageSent(batch.get(1));
		Thread.sleep(500);
		changeDAO.registerMessageSent(batch.get(2));
		Thread.sleep(500);
		List<ChangeMessage> notSent = changeDAO.listUnsentMessages(3);
		assertEquals(0, notSent.size());
		notProcessed = processedMessageDAO.listNotProcessedMessages("Q1", 3);
		assertEquals(3, notProcessed.size());
		// Register a processed msg for queue Q
		processedMessageDAO.registerMessageProcessed(batch.get(1).getChangeNumber(), "Q1");
		notProcessed = processedMessageDAO.listNotProcessedMessages("Q1", 3);
		assertEquals(2, notProcessed.size());
		// Register another processed msg for queue Q
		processedMessageDAO.registerMessageProcessed(batch.get(0).getChangeNumber(), "Q1");
		notProcessed = processedMessageDAO.listNotProcessedMessages("Q1", 3);
		assertEquals(1, notProcessed.size());
		notProcessed = processedMessageDAO.listNotProcessedMessages("Q2", 3);
		assertEquals(3, notProcessed.size());
		// Register same msg as processed for queue Q2
		processedMessageDAO.registerMessageProcessed(batch.get(0).getChangeNumber(), "Q2");
		notProcessed = processedMessageDAO.listNotProcessedMessages("Q2", 3);
		assertEquals(2, notProcessed.size());
	}
	
	@Test
	public void testListUnsentRange() {
		// Create a set of changes with change numbers like:
		// 0 _ 2 3 _ 5 6
		List<ChangeMessage> batch = createList(5, ObjectType.ENTITY);
		batch = changeDAO.replaceChange(batch);
		batch.add(changeDAO.replaceChange(batch.remove(1)));
		batch.add(changeDAO.replaceChange(batch.remove(3)));
		
		long min = changeDAO.getMinimumChangeNumber();
		long max = changeDAO.getCurrentChangeNumber();
		
		// Get everything
		List<ChangeMessage> unSent = changeDAO.listUnsentMessages(min, max, new Timestamp(System.currentTimeMillis())); 
		assertEquals(batch, unSent);
		
		// Shrink the range and check each iteration for correctness
		for (int i = 0; i < batch.size(); i++) {
			if (i % 2 == 0) {
				ChangeMessage removed = batch.remove(0);
				min = removed.getChangeNumber() + 1;
			} else {
				ChangeMessage removed = batch.remove(batch.size() - 1);
				max = removed.getChangeNumber() - 1;
			}
			unSent = changeDAO.listUnsentMessages(min, max, new Timestamp(System.currentTimeMillis())); 
			assertEquals(batch, unSent);
		}
	}
	
	/**
	 * Duplicate entry for key 'CHANGES_UKEY_OID_OTYPE' can occur with fast conccurent updates
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testPLFM2756() throws InterruptedException, ExecutionException{
		final List<ChangeMessage> toSpam = createList(1, ObjectType.TABLE);
		final int timesToRun = 100;
		Callable<Integer> callable = new Callable<Integer>(){
			@Override
			public Integer call() throws Exception {
				for(int i=0; i<timesToRun; i++){
					changeDAO.replaceChange(toSpam);
				}
				return timesToRun;
		}};
		// Run multiple threads at the same time
		ExecutorService pool = Executors.newFixedThreadPool(2);
		// Submit twice
		Future<Integer> one = pool.submit(callable);
		// Submit again
		Future<Integer> two = pool.submit(callable);
		// There should be no errors
		Integer oneResult = one.get();
		assertEquals(new Integer(timesToRun), oneResult);
		Integer twoResult = two.get();
		assertEquals(new Integer(timesToRun), twoResult);
	}
	
	@Test
	public void testCheckUnsentMessageByCheckSumForRange(){
		List<ChangeMessage> starting = createList(5, ObjectType.PRINCIPAL);
		starting = changeDAO.replaceChange(starting);
		assertFalse("The check-sums should not match",changeDAO.checkUnsentMessageByCheckSumForRange(0L, Long.MAX_VALUE));
		// Send each
		for(ChangeMessage toSend: starting){
			assertFalse("The check-sums should not match",changeDAO.checkUnsentMessageByCheckSumForRange(0L, Long.MAX_VALUE));
			changeDAO.registerMessageSent(toSend);
		}
		// The should match now that all are sent
		assertTrue("Change and sent are in-synch so their check-sums should match",changeDAO.checkUnsentMessageByCheckSumForRange(0L, Long.MAX_VALUE));
	}
	
	/**
	 * Will add a row to start the a test.
	 * @return
	 */
	public Long startChangeNumber(){
		List<ChangeMessage> starting = createList(1, ObjectType.PRINCIPAL);
		starting = changeDAO.replaceChange(starting);
		return starting.get(0).getChangeNumber();
	}

	/**
	 *  Do not ignore this test.  If it starts to fail then that means it is broken even
	 *  if the failures are sporadic.
	 *  See: PLFM-1659, PLFM-1631, and PLFM-2860.
	 */
	@Test
	public void testForDeadlocks() throws Exception {
		final int numOfTasks = 1000;
		final int numOfThreads = 10;
		// Create the list of tasks
		List<ReplaceChange> taskList = new ArrayList<ReplaceChange>(numOfTasks);
		for (int i = 0; i < 100; i++) {
			ChangeMessage change = new ChangeMessage();
			change.setObjectId("829165202913" + (i % 3)); // Simulate gap lock on the OBJECT_ID column
			change.setObjectType(ObjectType.ENTITY);
			change.setObjectEtag(Long.toString(System.currentTimeMillis()));
			change.setChangeType(ChangeType.UPDATE);
			change.setTimestamp(new Date());
			taskList.add(new ReplaceChange(change));
		}
		// Send the list of changes to a pool of threads
		ExecutorService exe = Executors.newFixedThreadPool(numOfThreads);
		try {
			List<Future<Boolean>> results = exe.invokeAll(taskList, 60, TimeUnit.SECONDS);
			for (Future<Boolean> future : results) {
				if (!future.get()) {
					Assert.fail("Deadlock detected.");
				}
			}
		} finally {
			exe.shutdown();
			exe.awaitTermination(60, TimeUnit.SECONDS);
		}
	}

	/**
	 * Helper to build up a list of changes.
	 * @param numChangesInBatch
	 * @return
	 */
	private List<ChangeMessage> createList(int numChangesInBatch, ObjectType type) {
		List<ChangeMessage> batch = new ArrayList<ChangeMessage>();
		for(int i=0; i<numChangesInBatch; i++){
			ChangeMessage change = new ChangeMessage();
			if(ObjectType.ENTITY == type){
				change.setObjectId("syn"+i);
			}else{
				change.setObjectId(""+i);
			}
			change.setObjectEtag("etag"+i);
			change.setChangeType(ChangeType.UPDATE);
			change.setObjectType(type);
			batch.add(change);
		}
		return batch;
	}

	private class ReplaceChange implements Callable<Boolean> {
		private final ChangeMessage change;
		private ReplaceChange(ChangeMessage change) {
			this.change = change;
		}
		@Override
		public Boolean call() throws Exception {
			try {
				ChangeMessage result = changeDAO.replaceChange(change);
				change.setChangeNumber(result.getChangeNumber());
				changeDAO.registerMessageSent(change);
				return Boolean.TRUE;
			} catch (DeadlockLoserDataAccessException e) {
				return Boolean.FALSE;
			}
		}
	}
}
