package org.sagebionetworks.usagemetrics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.sagebionetworks.client.Synapse;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.EntityHeader;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.UserProfile;

public class DataAccessAudit {

	public static void main(String[] args) throws Exception {
		Synapse synapse = new Synapse();
		String username = args[0];
		String password = args[1];
		synapse.login(username, password);
		File idFile = new File(args[2]);
		
		List<String> projectIds = getIdsFromFile(idFile);
		
		getAclList(synapse, projectIds);
		
	}

	private static void getAclList(Synapse synapse, List<String> projectIds)
			throws SynapseException, IOException {
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File("/home/geoff/Documents/output.txt")));
		
		for (String entityId : projectIds) {

			AccessControlList acl;
			try {
				acl = synapse.getACL(entityId);
			} catch (SynapseNotFoundException e) {
				try {
				EntityHeader benefactor = synapse.getEntityBenefactor(entityId);
				acl = synapse.getACL(benefactor.getId());
				} catch (SynapseNotFoundException e2) {
					bufferedWriter.write(String.format("Skipping: %s%n", entityId));
					continue;
				}
			}
			Set<ResourceAccess> resourceAccess = acl.getResourceAccess();
			bufferedWriter.write(String.format("Entity: %s%n", entityId));
			
			for (ResourceAccess access : resourceAccess) {
				String profileName;
				try {
					UserProfile userProfile = synapse.getUserProfile(access
							.getPrincipalId().toString());
					profileName = userProfile.getDisplayName();
				} catch (SynapseNotFoundException e) {
					profileName = access.getPrincipalId().toString();
				}
				bufferedWriter.write(String.format("\t%s - %s - %s%n", access.getGroupName(),
						profileName, access.getAccessType()));
			}
		}
	}

	private static List<String> getIdsFromFile(File idListFile) throws Exception {
		ArrayList<String> entityList = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new FileReader(idListFile));
		
		String line;
		while ((line = reader.readLine()) != null) {
			entityList.add(line);
		}
		return entityList;
	}
}
