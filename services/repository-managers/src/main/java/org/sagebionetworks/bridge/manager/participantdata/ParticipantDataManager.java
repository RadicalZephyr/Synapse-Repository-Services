package org.sagebionetworks.bridge.manager.participantdata;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.List;

import org.sagebionetworks.bridge.model.ParticipantDataId;
import org.sagebionetworks.bridge.model.data.ParticipantDataCurrentRow;
import org.sagebionetworks.bridge.model.data.ParticipantDataRow;
import org.sagebionetworks.bridge.model.timeseries.TimeSeriesTable;
import org.sagebionetworks.repo.model.DatastoreException;
import org.sagebionetworks.repo.model.IdList;
import org.sagebionetworks.repo.model.PaginatedResults;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.web.NotFoundException;

public interface ParticipantDataManager {

	public enum SortType {
		SORT_BY_DATE, SORT_BY_GROUP_AND_DATE
	};

	List<ParticipantDataRow> appendData(UserInfo userInfo, String participantDataId, List<ParticipantDataRow> data)
			throws DatastoreException, NotFoundException, IOException, GeneralSecurityException;

	List<ParticipantDataRow> appendData(UserInfo userInfo, ParticipantDataId participantId, String participantDataId,
			List<ParticipantDataRow> data) throws DatastoreException, NotFoundException, IOException;

	List<ParticipantDataRow> updateData(UserInfo userInfo, String participantDataId, List<ParticipantDataRow> data)
			throws DatastoreException, NotFoundException, IOException, GeneralSecurityException;

	void deleteRows(UserInfo userInfo, String participantDataId, IdList rowIds) throws IOException, NotFoundException,
			GeneralSecurityException;

	PaginatedResults<ParticipantDataRow> getData(UserInfo userInfo, String participantDataId, Integer limit,
			Integer offset, boolean normalizeData) throws DatastoreException, NotFoundException, IOException,
			GeneralSecurityException;

	List<ParticipantDataRow> getHistoryData(UserInfo userInfo, String participantDataId, boolean filterOutNotEnded,
			Date after, Date before, SortType sortType, boolean normalizeData) throws DatastoreException,
			NotFoundException, IOException, GeneralSecurityException;

	ParticipantDataCurrentRow getCurrentData(UserInfo userInfo, String participantDataId, boolean normalizeData)
			throws DatastoreException, NotFoundException, IOException, GeneralSecurityException;

	ParticipantDataRow getDataRow(UserInfo userInfo, String participantDataId, Long rowId, boolean normalizeData)
			throws DatastoreException, NotFoundException, IOException, GeneralSecurityException;

	TimeSeriesTable getTimeSeries(UserInfo userInfo, String participantDataId, List<String> columnNames,
			boolean normalizeData) throws DatastoreException, NotFoundException, IOException, GeneralSecurityException;
}
