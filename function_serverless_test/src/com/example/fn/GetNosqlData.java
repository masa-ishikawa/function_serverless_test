package com.example.fn;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.fnproject.fn.api.FnConfiguration;
import com.fnproject.fn.api.RuntimeContext;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.model.internal.JsonConverter;
import com.oracle.bmc.nosql.NosqlClient;
import com.oracle.bmc.nosql.model.QueryDetails;
import com.oracle.bmc.nosql.model.QueryResultCollection;
import com.oracle.bmc.nosql.requests.QueryRequest;
import com.oracle.bmc.nosql.responses.QueryResponse;

public class GetNosqlData {

	private String compartmentId;
	private String regionId;

	@FnConfiguration
	public void config(RuntimeContext ctx) {
		compartmentId = ctx.getConfigurationByKey("COMPARTMENT_ID").orElse("");
		regionId = ctx.getConfigurationByKey("REGION_ID").orElse("");
	}

	/**
	 * @return A NosqlClient
	 */
	private NosqlClient getNosqlClient() throws IOException {

		final ResourcePrincipalAuthenticationDetailsProvider provider = ResourcePrincipalAuthenticationDetailsProvider
				.builder().build();
		final NosqlClient nosqlClient = new NosqlClient(provider);
		nosqlClient.setRegion(Region.fromRegionId(regionId));
		return nosqlClient;
	}

	private List<Map<String, Object>> makeQuery(NosqlClient client, String owner) {

		Logger log = Logger.getLogger("");
		String sql = "select * from JavaQuickstart";
		if (!owner.equals(""))
			sql += " where owner = \'" + owner + "\'";
		log.info(sql);
		QueryDetails qd = QueryDetails.builder()
				.compartmentId(compartmentId)
				.timeoutInMs(5000)
				.statement(sql)
				.build();
		QueryRequest qreq = QueryRequest.builder()
				.queryDetails(qd)
				.build();
		QueryResponse qres = client.query(qreq);
		QueryResultCollection qrc = qres.getQueryResultCollection();
		return qrc.getItems();

	}

	public String handleRequest(String owner) throws Exception {

		//System.out.println("Inside Java HelloNosqlFunction.handleRequest()");
		try {
			NosqlClient nosqlClient = getNosqlClient();
			List<Map<String, Object>> list = makeQuery(nosqlClient, owner);
			return JsonConverter.objectToJsonBlob(list);

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}
