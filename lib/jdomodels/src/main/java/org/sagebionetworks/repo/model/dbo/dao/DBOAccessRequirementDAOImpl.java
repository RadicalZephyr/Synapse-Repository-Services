/**
 * 
 */
package org.sagebionetworks.repo.model.dbo.dao;

import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_ACCESS_REQUIREMENT_ETAG;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_ACCESS_REQUIREMENT_ID;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_NODE_ACCESS_REQUIREMENT_NODE_ID;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.COL_NODE_ACCESS_REQUIREMENT_REQUIREMENT_ID;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.LIMIT_PARAM_NAME;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.OFFSET_PARAM_NAME;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.TABLE_ACCESS_REQUIREMENT;
import static org.sagebionetworks.repo.model.query.jdo.SqlConstants.TABLE_NODE_ACCESS_REQUIREMENT;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.sagebionetworks.ids.IdGenerator;
import org.sagebionetworks.repo.model.AccessRequirement;
import org.sagebionetworks.repo.model.AccessRequirementDAO;
import org.sagebionetworks.repo.model.ConflictingUpdateException;
import org.sagebionetworks.repo.model.DatastoreException;
import org.sagebionetworks.repo.model.InvalidModelException;
import org.sagebionetworks.repo.model.ObjectData;
import org.sagebionetworks.repo.model.ObjectDescriptor;
import org.sagebionetworks.repo.model.QueryResults;
import org.sagebionetworks.repo.model.dbo.DBOBasicDao;
import org.sagebionetworks.repo.model.dbo.TableMapping;
import org.sagebionetworks.repo.model.dbo.persistence.DBOAccessRequirement;
import org.sagebionetworks.repo.model.dbo.persistence.DBONodeAccessRequirement;
import org.sagebionetworks.repo.model.jdo.KeyFactory;
import org.sagebionetworks.repo.model.jdo.ObjectDescriptorUtils;
import org.sagebionetworks.repo.model.query.jdo.SqlConstants;
import org.sagebionetworks.repo.web.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;


/**
 * @author brucehoff
 *
 */
public class DBOAccessRequirementDAOImpl implements AccessRequirementDAO {
	@Autowired
	private DBOBasicDao basicDao;
	
	@Autowired
	private IdGenerator idGenerator;
	
	@Autowired
	private SimpleJdbcTemplate simpleJdbcTempalte;
	
	private static final String SELECT_FOR_NODE_SQL = 
			"SELECT * FROM "+SqlConstants.TABLE_ACCESS_REQUIREMENT+" ar, "+ 
			SqlConstants.TABLE_NODE_ACCESS_REQUIREMENT +" nar WHERE ar."+
			SqlConstants.COL_ACCESS_REQUIREMENT_ID+" = nar."+SqlConstants.COL_NODE_ACCESS_REQUIREMENT_REQUIREMENT_ID+
			" AND nar."+COL_NODE_ACCESS_REQUIREMENT_NODE_ID+"=:"+COL_NODE_ACCESS_REQUIREMENT_NODE_ID;
	
	private static final String SELECT_FOR_NAR_SQL = "select * from "+TABLE_NODE_ACCESS_REQUIREMENT+" where "+
	COL_NODE_ACCESS_REQUIREMENT_REQUIREMENT_ID+" = "+COL_NODE_ACCESS_REQUIREMENT_REQUIREMENT_ID;

	private static final String SELECT_FOR_RANGE_SQL = "select * from "+TABLE_ACCESS_REQUIREMENT+" order by "+COL_ACCESS_REQUIREMENT_ID+
	" limit :"+LIMIT_PARAM_NAME+" offset :"+OFFSET_PARAM_NAME;

	private static final String SELECT_FOR_MULTIPLE_NAR_SQL = "select * from "+TABLE_NODE_ACCESS_REQUIREMENT+" where "+
		COL_NODE_ACCESS_REQUIREMENT_REQUIREMENT_ID+" IN (:"+COL_NODE_ACCESS_REQUIREMENT_REQUIREMENT_ID+")";

	private static final String SELECT_FOR_UPDATE_SQL = "select * from "+TABLE_ACCESS_REQUIREMENT+" where "+COL_ACCESS_REQUIREMENT_ID+
			"=:"+COL_ACCESS_REQUIREMENT_ID+" for update";
	
	private static final String DELETE_NODE_ACCESS_REQUIREMENT_SQL = 
		"delete from "+TABLE_NODE_ACCESS_REQUIREMENT+" where "+
		COL_NODE_ACCESS_REQUIREMENT_REQUIREMENT_ID+"=:"+COL_NODE_ACCESS_REQUIREMENT_REQUIREMENT_ID;

	private static final TableMapping<DBOAccessRequirement> TABLE_MAPPING = (new DBOAccessRequirement()).getTableMapping();
	
	private static final RowMapper<DBOAccessRequirement> accessRequirementRowMapper = (new DBOAccessRequirement()).getTableMapping();
	private static final RowMapper<DBONodeAccessRequirement> nodeAccessRequirementRowMapper = (new DBONodeAccessRequirement()).getTableMapping();


	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	@Override
	public void delete(String id) throws DatastoreException, NotFoundException {
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue(COL_ACCESS_REQUIREMENT_ID.toLowerCase(), id);
		basicDao.deleteObjectById(DBOAccessRequirement.class, param);
	}

	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	@Override
	public <T extends AccessRequirement> T create(T dto) throws DatastoreException, InvalidModelException{
	
		DBOAccessRequirement jdo = new DBOAccessRequirement();
		AccessRequirementUtils.copyDtoToDbo(dto, jdo);
		if (jdo.geteTag()==null) jdo.seteTag(0L);
		jdo.setId(idGenerator.generateNewId());
		jdo = basicDao.createNew(jdo);
		populateNodeAccessRequirement(jdo.getId(), dto.getEntityIds());
		T result = (T)AccessRequirementUtils.copyDboToDto(jdo, getEntities(jdo.getId()));
		return result;
	}
	
	public void populateNodeAccessRequirement(Long accessRequirementId, List<String> entityIds) throws DatastoreException {
		if (entityIds==null || entityIds.isEmpty()) return;
		List<DBONodeAccessRequirement> batch = new ArrayList<DBONodeAccessRequirement>();
		for (String s: entityIds) {
			DBONodeAccessRequirement nar = new DBONodeAccessRequirement();
			nar.setAccessRequirementId(accessRequirementId);
			Long nodeId = KeyFactory.stringToKey(s);
			nar.setNodeId(nodeId);
			batch.add(nar);
		}
		basicDao.createBatch(batch);
	}

	@Transactional(readOnly = true)
	@Override
	public AccessRequirement get(String id) throws DatastoreException, NotFoundException {
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue(COL_ACCESS_REQUIREMENT_ID.toLowerCase(), id);
		DBOAccessRequirement dbo = basicDao.getObjectById(DBOAccessRequirement.class, param);
		List<Long> entities = getEntities(dbo.getId());
		AccessRequirement dto = AccessRequirementUtils.copyDboToDto(dbo, entities);
		return dto;
	}
	
	public List<Long> getEntities(Long accessRequirementId) {
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue(COL_NODE_ACCESS_REQUIREMENT_REQUIREMENT_ID, accessRequirementId);
		List<DBONodeAccessRequirement> nars = simpleJdbcTempalte.query(SELECT_FOR_NAR_SQL, nodeAccessRequirementRowMapper, param);
		List<Long> ans = new ArrayList<Long>();	
		for (DBONodeAccessRequirement nar: nars) ans.add(nar.getNodeId());
		return ans;
	}
	
	@Transactional(readOnly = true)
	@Override
	public long getCount() throws DatastoreException {
		return basicDao.getCount(DBOAccessRequirement.class);
	}

	/**
	 * 
	 */
	@Transactional(readOnly = true)
	@Override
	public QueryResults<ObjectData> getMigrationObjectData(long offset, long limit, boolean includeDependencies) throws DatastoreException {
		// (1) get one 'page' of AccessRequirements (just their IDs and Etags)
		List<ObjectData> ods = null;
		{
			MapSqlParameterSource param = new MapSqlParameterSource();
			param.addValue(OFFSET_PARAM_NAME, offset);
			param.addValue(LIMIT_PARAM_NAME, limit);
			ods = simpleJdbcTempalte.query(SELECT_FOR_RANGE_SQL, new RowMapper<ObjectData>() {

				@Override
				public ObjectData mapRow(ResultSet rs, int rowNum)
						throws SQLException {
					String id = rs.getString(COL_ACCESS_REQUIREMENT_ID);
					String etag = rs.getString(COL_ACCESS_REQUIREMENT_ETAG);
					ObjectData objectData = new ObjectData();
					ObjectDescriptor od = new ObjectDescriptor();
					od.setId(id);
					od.setType(AccessRequirement.class.getName());
					objectData.setId(od);
					objectData.setEtag(etag);
					objectData.setDependencies(new HashSet<ObjectDescriptor>());
					return objectData;
				}
			
			}, param);
		}
		
		// (2) find the dependencies
		if (includeDependencies && !ods.isEmpty()) {
			Map<String, ObjectData> arMap = new HashMap<String, ObjectData>();	
			for (ObjectData od: ods) arMap.put(od.getId().getId(), od);
			
			List<DBONodeAccessRequirement> nars = null;
			{
				MapSqlParameterSource param = new MapSqlParameterSource();
				param.addValue(COL_NODE_ACCESS_REQUIREMENT_REQUIREMENT_ID, arMap.keySet());
				nars = simpleJdbcTempalte.query(SELECT_FOR_MULTIPLE_NAR_SQL, nodeAccessRequirementRowMapper, param);
			}

			// (3) add the dependencies to the objects
			for (DBONodeAccessRequirement nar : nars) {
				ObjectDescriptor od = ObjectDescriptorUtils.createEntityObjectDescriptor(nar.getNodeId());
				ObjectData objectData = arMap.get(nar.getAccessRequirementId().toString());
				objectData.getDependencies().add(od);
			}
		}
		// (4) return the 'page' of objects, along with the total result count
		QueryResults<ObjectData> queryResults = new QueryResults<ObjectData>();
		queryResults.setResults(ods);
		queryResults.setTotalNumberOfResults((int)getCount());
		return queryResults;
	}

	@Transactional(readOnly = true)
	@Override
	public List<AccessRequirement> getForNode(String nodeId) throws DatastoreException {
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue(COL_NODE_ACCESS_REQUIREMENT_NODE_ID, KeyFactory.stringToKey(nodeId));
		List<DBOAccessRequirement> dbos = simpleJdbcTempalte.query(SELECT_FOR_NODE_SQL, accessRequirementRowMapper, param);
		List<AccessRequirement>  dtos = new ArrayList<AccessRequirement>();
		for (DBOAccessRequirement dbo : dbos) {
			AccessRequirement dto = AccessRequirementUtils.copyDboToDto(dbo, getEntities(dbo.getId()));
			dtos.add(dto);
		}
		return dtos;
	}
	
	
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	@Override
	public <T extends AccessRequirement> T update(T dto) throws DatastoreException,
			InvalidModelException,NotFoundException, ConflictingUpdateException {
		// LOCK the record
		DBOAccessRequirement dbo = null;
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue(COL_ACCESS_REQUIREMENT_ID, dto.getId());
		try{
			dbo = simpleJdbcTempalte.queryForObject(SELECT_FOR_UPDATE_SQL, TABLE_MAPPING, param);
		}catch (EmptyResultDataAccessException e) {
			throw new NotFoundException("The resource you are attempting to access cannot be found");
		}
		// check dbo's etag against dto's etag
		// if different rollback and throw a meaningful exception
		if (!dbo.geteTag().equals(Long.parseLong(dto.getEtag())))
			throw new ConflictingUpdateException("Access Requirement was updated since you last fetched it, retrieve it again and reapply the update.");
		AccessRequirementUtils.copyDtoToDbo(dto, dbo);
		dbo.seteTag(1L+dbo.geteTag());
		boolean success = basicDao.update(dbo);
		if (!success) throw new DatastoreException("Unsuccessful updating user Access Requirement in database.");
		updateNodeAccessRequirement(dbo.getId(), dto.getEntityIds());
		T updatedAR = (T)AccessRequirementUtils.copyDboToDto(dbo, getEntities(dbo.getId()));
		return updatedAR;
	} // the 'commit' is implicit in returning from a method annotated 'Transactional'
	
	// to update the node ids for the Access Requirement, we just delete the existing ones and repopulate
	private void updateNodeAccessRequirement(Long acessRequirementId, List<String> entityIds) throws DatastoreException {
		// delete the existing node-access-requirement records...
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue(COL_NODE_ACCESS_REQUIREMENT_REQUIREMENT_ID, acessRequirementId);
		simpleJdbcTempalte.update(DELETE_NODE_ACCESS_REQUIREMENT_SQL, param);
		// ... now populate with the updated values
		populateNodeAccessRequirement(acessRequirementId, entityIds);
	}
	
}
