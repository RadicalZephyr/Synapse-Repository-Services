package org.sagebionetworks.usagemetrics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sagebionetworks.client.Synapse;
import org.sagebionetworks.client.exceptions.SynapseException;

public class DataAuditStats {
	private static class DataObject {

		public DataObject(String entityId, String name, String etag, String createdBy,
				String modifiedBy) {
			this.entityId = entityId;
			this.name = name;
			this.etag = etag;
			this.createdBy = createdBy;
			this.modifiedBy = modifiedBy;
		}
		
		public String entityId;
		public String name;
		public String etag;
		public String createdBy;
		public String modifiedBy;
		
	}
	
	private static final String ID_TO_USERNAME_FILE = "/home/geoff/Downloads/principalIdToUserNameMap.csv";
	private static Map<String, String> idToUser;
	private static final int TIME_WINDOW_DAYS = 30;
	
	private static final boolean VERBOSE = false;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Long start = (new Date()).getTime()-TIME_WINDOW_DAYS*24*3600*1000L;
		Long end = null;

		Synapse synapse = new Synapse();
		String username = args[0];
		String password = args[1];
		synapse.login(username, password);

		initIdToEmailMap();
		
		ArrayList<DataObject> entities = getEntities(synapse);
		System.out.format("There a total of %d data entities possibly not created by Sage Employees%n%n", entities.size());
		
		for (DataObject obj : entities) {
			System.out.format("%s created by %s%n", obj.name, obj.createdBy);
		}
	}

	// Rewrite this method to be much smarter.  Several things: approach from the other direction.
	// filter the list of users for non-sage employees, then run per-user queries limiting data 
	// where createdBy = userId.  Second.  This step is potentially parallelizable, make that object a runnable
	// and then dispatch three or four threads with lists of users to process.
	public static ArrayList<DataObject> getEntities(Synapse synapse) throws SynapseException, JSONException {
		String baseQuery = "select id, name, eTag, createdByPrincipalId, modifiedByPrincipalId from data limit %d offset %d";
		
		JSONObject dataTotal = synapse.query("select id from data limit 1");

		int offset = 1;
		int batchSize = 20;
		int total = (int)dataTotal.getLong("totalNumberOfResults");
		
		ArrayList<DataObject> dataList = new ArrayList<DataObject>();
		
		while (offset < total) {
			JSONObject dataBatch = synapse.query(String.format(baseQuery, batchSize, offset));

			JSONArray d = dataBatch.getJSONArray("results");
			for (int j=0; j<d.length(); j++) {
				JSONObject data = d.getJSONObject(j);
				
				DataObject obj = new DataObject(data.getString("data.id"), 
												 data.getString("data.name"),
												 data.getString("data.eTag"),
												 idToUser.get(data.getString("data.createdByPrincipalId")),
												 idToUser.get(data.getString("data.modifiedByPrincipalId")));

				if (j==0  && VERBOSE) System.out.format("\t\t%s - data  %d of %d", obj.name, offset, total);
				if (isCreatedBySageEmployee(obj))
					dataList.add(obj);
			}

			offset += batchSize;
		}
		
		return dataList;
	}
	
	private static boolean isCreatedBySageEmployee(DataObject obj) {
		if (obj.createdBy == null)
			return false;
		
		String[] split = obj.createdBy.split("@");
		
		if (split.length != 2 || split[1].equalsIgnoreCase("sagebase.org")) {
			return false;
		}
		return true;
	}

	public static void initIdToEmailMap() {
		// Load the csv file and process it into the map.
		File file = new File(ID_TO_USERNAME_FILE);
		FileInputStream is;
		try {
			is = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		try {
			idToUser = new HashMap<String, String>(600);
			String s = br.readLine();
			while (s != null) {
				String[] values = s.split(",");
				try {
					if (Integer.parseInt(values[2]) == 1) {
						idToUser.put(values[0], values[3]);
					}
				} catch (NumberFormatException e) {
				}
				s = br.readLine();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}

}
