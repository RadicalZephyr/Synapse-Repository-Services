package org.sagebionetworks.repo.model.dbo.dao;

import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_CHANGES_CHANGE_NUM;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_CHANGES_OBJECT_ID;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_CHANGES_OBJECT_TYPE;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_CHANGES_TIME_STAMP;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_PROCESSED_MESSAGES_CHANGE_NUM;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_PROCESSED_MESSAGES_QUEUE_NAME;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_PROCESSED_MESSAGES_TIME_STAMP;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_SENT_MESSAGES_CHANGE_NUM;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_SENT_MESSAGES_OBJECT_ID;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_SENT_MESSAGES_OBJECT_TYPE;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_SENT_MESSAGES_TIME_STAMP;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.TABLE_CHANGES;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.TABLE_PROCESSED_MESSAGES;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.TABLE_SENT_MESSAGES;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.ids.IdGenerator;
import org.sagebionetworks.ids.IdGenerator.TYPE;
import org.sagebionetworks.repo.model.DatastoreException;
import org.sagebionetworks.repo.model.ObjectType;
import org.sagebionetworks.repo.model.dbo.DBOBasicDao;
import org.sagebionetworks.repo.model.dbo.TableMapping;
import org.sagebionetworks.repo.model.dbo.persistence.DBOChange;
import org.sagebionetworks.repo.model.dbo.persistence.DBOSentMessage;
import org.sagebionetworks.repo.model.jdo.KeyFactory;
import org.sagebionetworks.repo.model.message.ChangeMessage;
import org.sagebionetworks.repo.model.message.ChangeMessageUtils;
import org.sagebionetworks.repo.model.message.ChangeType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;


/**
 * The implementation of the change DBOChangeDAO
 * @author John
 *
 */
public class DBOChangeDAOImpl implements DBOChangeDAO {
	
	private static final String SQL_CHANGE_CHECK_SUM_FOR_RANGE = "SELECT SUM(CRC32("+COL_CHANGES_CHANGE_NUM+")) FROM "+TABLE_CHANGES+" WHERE "+COL_CHANGES_CHANGE_NUM+" >= ? AND "+COL_CHANGES_CHANGE_NUM+" <= ?";

	private static final String SQL_SENT_CHECK_SUM_FOR_RANGE = "SELECT SUM(CRC32("+COL_SENT_MESSAGES_CHANGE_NUM+")) FROM "+TABLE_SENT_MESSAGES+" WHERE "+COL_SENT_MESSAGES_CHANGE_NUM+" >= ? AND "+COL_SENT_MESSAGES_CHANGE_NUM+" <= ?";

	private static final String SQL_SELECT_MAX_SENT_CHANGE_NUMBER_LESS_THAN_OR_EQUAL = "SELECT MAX("+COL_SENT_MESSAGES_CHANGE_NUM+") FROM "+TABLE_SENT_MESSAGES+" WHERE "+COL_SENT_MESSAGES_CHANGE_NUM+" <= ?";

	static private Logger log = LogManager.getLogger(DBOChangeDAOImpl.class);
	
	private static final String SQL_INSERT_SENT_ON_DUPLICATE_UPDATE = 
			"INSERT INTO "+TABLE_SENT_MESSAGES+" ( "+COL_SENT_MESSAGES_CHANGE_NUM+", "+COL_SENT_MESSAGES_TIME_STAMP+")"+
			" VALUES ( ?, ?) ON DUPLICATE KEY UPDATE "+COL_SENT_MESSAGES_TIME_STAMP+" = ?";
	
	private static final String SQL_CHANGES_NOT_SENT_PREFIX = 
			"SELECT C.* FROM "+TABLE_CHANGES+
			" C LEFT OUTER JOIN "+TABLE_SENT_MESSAGES+" S ON (C."+COL_CHANGES_CHANGE_NUM+" = S."+COL_SENT_MESSAGES_CHANGE_NUM+")"+
			"WHERE S."+COL_SENT_MESSAGES_CHANGE_NUM+" IS NULL";
	
	private static final String SQL_SELECT_CHANGES_NOT_SENT = 
			SQL_CHANGES_NOT_SENT_PREFIX + " LIMIT ?";
	
	private static final String SQL_SELECT_CHANGES_NOT_SENT_IN_RANGE = 
			SQL_CHANGES_NOT_SENT_PREFIX+
			" AND C."+COL_SENT_MESSAGES_CHANGE_NUM+" >= ?"+
			" AND C."+COL_SENT_MESSAGES_CHANGE_NUM+" <= ?"+
			" AND C."+COL_CHANGES_TIME_STAMP+" <= ?"+
			" ORDER BY "+COL_SENT_MESSAGES_CHANGE_NUM;
	
	private static final String SELECT_CHANGE_NUMBER_FOR_OBJECT_ID_AND_TYPE = 
			"SELECT "+COL_CHANGES_CHANGE_NUM+" FROM "+TABLE_CHANGES+
			" WHERE "+COL_CHANGES_OBJECT_ID+" = ? AND "+COL_CHANGES_OBJECT_TYPE+" = ?";

	private static final String SQL_SELECT_ALL_GREATER_THAN_OR_EQUAL_TO_CHANGE_NUMBER_FILTER_BY_OBJECT_TYPE = 
			"SELECT * FROM "+TABLE_CHANGES+
			" WHERE "+COL_CHANGES_CHANGE_NUM+" >= ? AND "+COL_CHANGES_OBJECT_TYPE+" = ?"+
			" ORDER BY "+COL_CHANGES_CHANGE_NUM+" ASC LIMIT ?";

	private static final String SQL_SELECT_ALL_GREATER_THAN_OR_EQUAL_TO_CHANGE_NUMBER = 
			"SELECT * FROM "+TABLE_CHANGES+" WHERE "+COL_CHANGES_CHANGE_NUM+" >= ? ORDER BY "+COL_CHANGES_CHANGE_NUM+" ASC LIMIT ?";

	private static final String SQL_SELECT_MIN_CHANGE_NUMBER = 
			"SELECT MIN("+COL_CHANGES_CHANGE_NUM+") FROM "+TABLE_CHANGES;
	
	private static final String SQL_SELECT_MAX_CHANGE_NUMBER = 
			"SELECT MAX("+COL_CHANGES_CHANGE_NUM+") FROM "+TABLE_CHANGES;
	
	private static final String SQL_SELECT_COUNT_CHANGE_NUMBER = 
			"SELECT COUNT("+COL_CHANGES_CHANGE_NUM+") FROM "+TABLE_CHANGES;

	private static final String SQL_DELETE_BY_CHANGE_NUM = 
			"DELETE FROM "+TABLE_CHANGES+" WHERE "+COL_CHANGES_CHANGE_NUM+" = ?";

	private static final String SQL_INSERT_PROCESSED_ON_DUPLICATE_UPDATE =
			"INSERT INTO "+TABLE_PROCESSED_MESSAGES+" ( "+COL_PROCESSED_MESSAGES_CHANGE_NUM+", "+COL_PROCESSED_MESSAGES_QUEUE_NAME+", "+COL_PROCESSED_MESSAGES_TIME_STAMP+") VALUES ( ?, ?, ?) ON DUPLICATE KEY UPDATE "+COL_SENT_MESSAGES_TIME_STAMP+" = ?";

	private static final String SQL_SELECT_CHANGES_NOT_PROCESSED =
			"select c.* from " + TABLE_CHANGES + " c join " + TABLE_SENT_MESSAGES + " s on s." + COL_SENT_MESSAGES_CHANGE_NUM + " = c." + COL_CHANGES_CHANGE_NUM + " left join (select * from " + TABLE_PROCESSED_MESSAGES + " where " + COL_PROCESSED_MESSAGES_QUEUE_NAME + " = ?) p on p." + COL_PROCESSED_MESSAGES_CHANGE_NUM + " = s." + COL_SENT_MESSAGES_CHANGE_NUM + " where p." + COL_PROCESSED_MESSAGES_CHANGE_NUM + " is null limit ?";
	
	private static final String SQL_SENT_CHANGE_NUMBER_FOR_UPDATE = "SELECT "+COL_SENT_MESSAGES_CHANGE_NUM+" FROM "+TABLE_SENT_MESSAGES+" WHERE "+COL_SENT_MESSAGES_OBJECT_ID+" = ? AND "+COL_SENT_MESSAGES_OBJECT_TYPE+" = ? FOR UPDATE";
	private static final String CREATE_OR_UPDATE_SENT_MESSAGE = "INSERT INTO "+TABLE_SENT_MESSAGES+"("+COL_SENT_MESSAGES_CHANGE_NUM+", "+COL_SENT_MESSAGES_OBJECT_ID+", "+COL_SENT_MESSAGES_OBJECT_TYPE+") VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE "+COL_SENT_MESSAGES_CHANGE_NUM+" = ?";

	@Autowired
	private DBOBasicDao basicDao;

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private IdGenerator idGenerator;
	
	private TableMapping<DBOChange> rowMapper = new DBOChange().getTableMapping();

	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	@Override
	public ChangeMessage replaceChange(ChangeMessage change) {
		if(change == null) throw new IllegalArgumentException("DBOChange cannot be null");
		if(change.getObjectId() == null) throw new IllegalArgumentException("change.getObjectId() cannot be null");
		if(change.getChangeType() == null) throw new IllegalArgumentException("change.getChangeTypeEnum() cannot be null");
		if(change.getObjectType() == null) throw new IllegalArgumentException("change.getObjectTypeEnum() cannot be null");
		if(ChangeType.CREATE == change.getChangeType() && change.getObjectEtag() == null) throw new IllegalArgumentException("Etag cannot be null for ChangeType: "+change.getChangeType());
		if(ChangeType.UPDATE == change.getChangeType() && change.getObjectEtag() == null) throw new IllegalArgumentException("Etag cannot be null for ChangeType: "+change.getChangeType());
		DBOChange dbo = ChangeMessageUtils.createDBO(change);
		// Clear the time stamp so that we get a new one automatically.
		// Note: Mysql TIMESTAMP only keeps seconds (not MS) so for consistency we only write second accuracy.
		// We are using (System.currentTimeMillis()/1000)*1000; to convert all MS to zeros.
		long nowMs = (System.currentTimeMillis()/1000)*1000;
		dbo.setChangeNumber(idGenerator.generateNewId(TYPE.CHANGE_ID));
		dbo.setTimeStamp(new Timestamp(nowMs));
		// Now insert the row with the current value
		dbo = basicDao.createOrUpdate(dbo);
		return ChangeMessageUtils.createDTO(dbo);
	}

	
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	@Override
	public List<ChangeMessage> replaceChange(List<ChangeMessage> batchDTO) {
		if(batchDTO == null) throw new IllegalArgumentException("Batch cannot be null");
		// To prevent deadlock we sort by object id to guarantee a consistent update order.
		batchDTO = ChangeMessageUtils.sortByObjectId(batchDTO);
		// Send each replace them in order.
		List<ChangeMessage> resutls = new ArrayList<ChangeMessage>();
		for(ChangeMessage change: batchDTO){
			resutls.add(replaceChange(change));
		}
		return resutls;
	}

	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	@Override
	public void deleteChange(Long objectId, ObjectType type) {
		if(objectId == null) throw new IllegalArgumentException("ObjectId cannot be null");
		if(type == null) throw new IllegalArgumentException("ObjectType cannot be null");
		// To avoid gap locking we do not delete by the ID.  Rather we get the primary key, then delete by the primary key.
		List<Long> list = jdbcTemplate.query(SELECT_CHANGE_NUMBER_FOR_OBJECT_ID_AND_TYPE, new RowMapper<Long>(){
			@Override
			public Long mapRow(ResultSet rs, int rowNum) throws SQLException {
				return rs.getLong(COL_CHANGES_CHANGE_NUM);
			}},objectId, type.name());
		// Log the error case where multiple are found.
		if(list.size() > 1){
			log.error("Found multiple rows with ObjectId: "+objectId+" and ObjectType type "+type);
		}
		// Even if there are multiple, delete them all
		for(Long primaryKey: list){
			// Unless there is an error there will only be one here.
			jdbcTemplate.update(SQL_DELETE_BY_CHANGE_NUM, primaryKey);
		}
	}

	@Override
	public long getMinimumChangeNumber() {
		return jdbcTemplate.queryForLong(SQL_SELECT_MIN_CHANGE_NUMBER);
	}
	
	@Override
	public long getCurrentChangeNumber() {
		return jdbcTemplate.queryForLong(SQL_SELECT_MAX_CHANGE_NUMBER);
	}
	
	@Override
	public long getCount() {
		return jdbcTemplate.queryForLong(SQL_SELECT_COUNT_CHANGE_NUMBER);
	}

	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	@Override
	public void deleteAllChanges() {
		jdbcTemplate.update("DELETE FROM  "+TABLE_CHANGES+" WHERE "+COL_CHANGES_CHANGE_NUM+" > -1");
	}

	@Override
	public List<ChangeMessage> listChanges(long greaterOrEqualChangeNumber, ObjectType type, long limit) {
		if(limit < 0) throw new IllegalArgumentException("Limit cannot be less than zero");
		List<DBOChange> dboList = null;
		if(type == null){
			// There is no type filter.
			dboList = jdbcTemplate.query(SQL_SELECT_ALL_GREATER_THAN_OR_EQUAL_TO_CHANGE_NUMBER, new DBOChange().getTableMapping(), greaterOrEqualChangeNumber, limit);
		}else{
			// Filter by object type.
			dboList = jdbcTemplate.query(SQL_SELECT_ALL_GREATER_THAN_OR_EQUAL_TO_CHANGE_NUMBER_FILTER_BY_OBJECT_TYPE, rowMapper, greaterOrEqualChangeNumber, type.name(), limit);
		}
		return ChangeMessageUtils.createDTOList(dboList);
	}


	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	@Override
	public boolean registerMessageSent(ChangeMessage message) {
		DBOSentMessage sent = new DBOSentMessage();
		sent.setChangeNumber(message.getChangeNumber());
		sent.setObjectId(KeyFactory.stringToKey(message.getObjectId()));
		sent.setObjectType(message.getObjectType());
		Long currentChangeNumber = selectSentChangeNumberForUpdate(sent);
		if(currentChangeNumber == null){
			try {
				this.basicDao.createNew(sent);
			} catch (Exception e) {
				/* Create new can fail with several errors depending on the exact condition of the failure.
				 * When two separate transaction try to insert the same row at the same time a Deadlock exception can occur.
				 * If one thread commits before the other thread starts, a duplicate key exception can occur.
				 * In all cases it means this thread did not succeed in setting the row, so false is returned.
				 */
				log.warn("Failed to create a new sent-message for change: "+message.toString()+" message: "+e.getMessage());
				return false;
			}
		}else{
			this.basicDao.update(sent);
		}
		return !sent.getChangeNumber().equals(currentChangeNumber);
	}


	/**
	 * Select the current change number of a sent message for update.
	 * @param sent
	 * @return
	 */
	public Long selectSentChangeNumberForUpdate(DBOSentMessage sent) {
		try {
			return this.jdbcTemplate.queryForObject(SQL_SENT_CHANGE_NUMBER_FOR_UPDATE, new SingleColumnRowMapper<Long>(), sent.getObjectId(), sent.getObjectType().name());
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	
	@Override
	public List<ChangeMessage> listUnsentMessages(long limit) {
		List<DBOChange> dboList = jdbcTemplate.query(SQL_SELECT_CHANGES_NOT_SENT, rowMapper, limit);
		return ChangeMessageUtils.createDTOList(dboList);
	}


	@Override
	public List<ChangeMessage> listUnsentMessages(long lowerBound,
			long upperBound, Timestamp olderThan) {
		List<DBOChange> dboList = jdbcTemplate.query(SQL_SELECT_CHANGES_NOT_SENT_IN_RANGE, rowMapper, lowerBound, upperBound, olderThan);
		return ChangeMessageUtils.createDTOList(dboList);
	}

	@Override
	public void registerMessageProcessed(long changeNumber, String queueName) {
		jdbcTemplate.update(SQL_INSERT_PROCESSED_ON_DUPLICATE_UPDATE, changeNumber, queueName, null, null);
		
	}

	@Override
	public List<ChangeMessage> listNotProcessedMessages(String queueName, long limit) {
		List<DBOChange> l = jdbcTemplate.query(SQL_SELECT_CHANGES_NOT_PROCESSED, new DBOChange().getTableMapping(), queueName, limit);
		return ChangeMessageUtils.createDTOList(l);
	}

	@Override
	public Long getMaxSentChangeNumber(Long lessThanOrEqual) {
		Long max = jdbcTemplate.queryForObject(SQL_SELECT_MAX_SENT_CHANGE_NUMBER_LESS_THAN_OR_EQUAL, new SingleColumnRowMapper<Long>(), lessThanOrEqual);
		if(max == null){
			max = new Long(-1);
		}
		return max;
	}


	@Override
	public boolean checkUnsentMessageByCheckSumForRange(long lowerBound,
			long upperBound) {
		BigDecimal sentCheckSum = jdbcTemplate.queryForObject(SQL_SENT_CHECK_SUM_FOR_RANGE,new SingleColumnRowMapper<BigDecimal>(), lowerBound, upperBound);
		BigDecimal changeCheckSum = jdbcTemplate.queryForObject(SQL_CHANGE_CHECK_SUM_FOR_RANGE,new SingleColumnRowMapper<BigDecimal>(), lowerBound, upperBound);
		if(sentCheckSum == null){
			if(changeCheckSum == null){
				return true;
			}else{
				return false;
			}
		}
		return sentCheckSum.equals(changeCheckSum);
	}

}
