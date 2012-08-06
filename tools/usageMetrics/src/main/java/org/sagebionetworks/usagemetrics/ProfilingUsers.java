package org.sagebionetworks.usagemetrics;

import org.sagebionetworks.client.Synapse;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.EntityBundle;
import org.sagebionetworks.repo.model.PaginatedResults;
import org.sagebionetworks.repo.model.UserProfile;

public class ProfilingUsers {
	private static final long NANOSECOND_PER_MILLISECOND = 1000000L;

	public static void main(String[] args) throws Exception {
		Synapse synapse = new Synapse();
		String username = args[0];
		String password = args[1];
		synapse.login(username, password);
		
		profileEntityBundle(synapse);

		
		
	}

	private static void profileEntityBundle(Synapse synapse)
			throws SynapseException {
		long start = System.nanoTime();
		PaginatedResults<UserProfile> users = synapse.getUsers(0, 600);
		long end = System.nanoTime();
		
		long latencyMS = (end - start) / NANOSECOND_PER_MILLISECOND;

		System.out.format("Entity bundle took: %dms%n", latencyMS);			
		
	}
}
