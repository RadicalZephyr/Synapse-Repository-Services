package org.sagebionetworks.client;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.sagebionetworks.client.exceptions.SynapseException;

/**
 * @author deflaux
 * 
 */
public class SynapseWikiGenerator extends SynapseAdministration {

	private static final Logger log = Logger
			.getLogger(SynapseWikiGenerator.class.getName());

	private static final String FAKE_SESSION_TOKEN = "XXXXXX";

	private static final String SESSION_TOKEN_RESPONSE_REGEX = SESSION_TOKEN_HEADER + "\": \"[^\"]+\"";

	private static final String FAKE_SESSION_RESPONSE = "\"" + SESSION_TOKEN_HEADER + "\" : \"" + FAKE_SESSION_TOKEN + "\"";

	private String username;
	private String password;

	/**
	 * @param args
	 * @return SynapseWikiGenerator
	 * @throws Exception
	 */
	public static SynapseWikiGenerator createFromArgs(String args[])
			throws Exception {

		Options options = new Options();
		options
				.addOption(
						"e",
						"repoEndpoint",
						true,
						"the repository service endpoint (e.g. https://repositoryservice.sagebase.org/repo/v1)");
		options
				.addOption(
						"a",
						"authEndpoint",
						true,
						"the authentication service endpoint (e.g. https://staging-auth.elasticbeanstalk.com/auth/v1)");
		options.addOption("u", "username", true,
				"the Synapse username (e.g. first.last@sagebase.org)");
		options.addOption("p", "password", true, "the Synapse password");
		options.addOption("h", "help", false, "print this usage message");

		String repoEndpoint = null;
		String authEndpoint = null;
		String username = null;
		String password = null;

		try {
			CommandLineParser parser = new PosixParser();
			CommandLine line = parser.parse(options, args);

			if (line.hasOption("help")) {
				HelpFormatter helpFormatter = new HelpFormatter();
				helpFormatter.setWidth(80);
				helpFormatter.printHelp("wiki generator tool",
						"how to use this tool", options, "have fun!");
				System.exit(0);
			}

			if (line.hasOption("repoEndpoint")) {
				repoEndpoint = line.getOptionValue("repoEndpoint");
			}

			if (line.hasOption("authEndpoint")) {
				authEndpoint = line.getOptionValue("authEndpoint");
			}

			if (line.hasOption("username") && line.hasOption("password")) {
				username = line.getOptionValue("username");
				password = line.getOptionValue("password");
			} else {
				throw new ParseException("missing required arguments");
			}
		} catch (ParseException exp) {
			HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.setWidth(80);
			helpFormatter.printHelp("wiki generator tool", exp.getMessage(),
					options, "have fun!");
			System.exit(1);
		}

		return new SynapseWikiGenerator(repoEndpoint, authEndpoint, username,
				password);
	}

	/**
	 * Default constructor, connects to prod
	 */
	public SynapseWikiGenerator() {
		this.username = null;
		this.password = null;
	}

	/**
	 * @param username
	 * @param password
	 * @throws Exception
	 */
	public SynapseWikiGenerator(String username, String password)
			throws Exception {
		this.username = username;
		this.password = password;
	}

	/**
	 * @param repoEndpoint
	 * @param authEndpoint
	 * @param username
	 * @param password
	 * @throws Exception
	 */
	public SynapseWikiGenerator(String repoEndpoint, String authEndpoint,
			String username, String password) throws Exception {

		this.username = username;
		this.password = password;

		if (null != repoEndpoint) {
			super.setRepositoryEndpoint(this.repoEndpoint);
		}
		if (null != authEndpoint) {
			super.setAuthEndpoint(this.authEndpoint);
		}
	}

	/**
	 * Log in using credentials we parsed from the command line arguments
	 * 
	 * @throws SynapseException
	 */
	public void login() throws SynapseException {
		this.login(username, password);
	}

	protected JSONObject dispatchSynapseRequest(String endpoint, String uri,
			String requestMethod, String requestContent,
			Map<String, String> requestHeaders) throws SynapseException {

		try {
			JSONObject requestObject = null;
			String curl = "curl -i ";

			if (null != requestContent) {
				requestObject = new JSONObject(requestContent);
			}

			if ("POST".equals(requestMethod)) {
				curl += " -d '" + requestObject.toString(JSON_INDENT) + "' ";
			} else if ("PUT".equals(requestMethod)) {
				curl += " -X PUT -d '" + requestObject.toString(JSON_INDENT)
						+ "' ";
			} else if ("DELETE".equals(requestMethod)) {
				curl += " -X DELETE ";
			}

			for (Entry<String, String> header : requestHeaders.entrySet()) {
				if (SESSION_TOKEN_HEADER.equals(header.getKey())) {
					curl += " -H " + header.getKey() + ":" + FAKE_SESSION_TOKEN;
				} else {
					curl += " -H " + header.getKey() + ":" + header.getValue();
				}
			}
			curl += " '" + uri + "'{code}";
			log.info("*Request* {code}" + curl);
			log.info("*Response* {code}");

			JSONObject responseObject = super.dispatchSynapseRequest(endpoint,
					uri, requestMethod, requestContent, requestHeaders);

			String response = responseObject.toString(JSON_INDENT);
			response = response.replaceAll(SESSION_TOKEN_RESPONSE_REGEX, FAKE_SESSION_RESPONSE);
			log.info(response + "{code}");

			return responseObject;
		} catch (JSONException e) {
			throw new SynapseException(e);
		}
	}

}