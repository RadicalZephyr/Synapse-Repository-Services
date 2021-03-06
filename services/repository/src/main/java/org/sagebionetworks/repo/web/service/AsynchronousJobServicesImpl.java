package org.sagebionetworks.repo.web.service;

import org.sagebionetworks.repo.manager.UserManager;
import org.sagebionetworks.repo.manager.asynch.AsynchJobStatusManager;
import org.sagebionetworks.repo.model.UserInfo;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.asynch.AsynchronousRequestBody;
import org.sagebionetworks.repo.web.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Basic implementation.
 * 
 * @author John
 *
 */
public class AsynchronousJobServicesImpl implements AsynchronousJobServices {
	
	@Autowired
	private UserManager userManager;
	@Autowired
	private AsynchJobStatusManager asynchJobStatusManager;

	@Override
	public AsynchronousJobStatus startJob(Long userId, AsynchronousRequestBody body) throws NotFoundException {
		if(userId == null) throw new IllegalArgumentException("UserId cannot be null");
		if(body == null) throw new IllegalArgumentException("Body cannot be null");
		UserInfo user = userManager.getUserInfo(userId);
		return asynchJobStatusManager.startJob(user, body);
	}

	@Override
	public AsynchronousJobStatus getJobStatus(Long userId, String jobId) throws NotFoundException {
		if(userId == null) throw new IllegalArgumentException("UserId cannot be null");
		if(jobId == null) throw new IllegalArgumentException("JobId cannot be null");
		UserInfo user = userManager.getUserInfo(userId);
		return asynchJobStatusManager.getJobStatus(user, jobId);
	}

}
