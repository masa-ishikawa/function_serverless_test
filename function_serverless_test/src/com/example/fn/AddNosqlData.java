package com.example.fn;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
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

public class AddNosqlData {

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

	public String handleRequest(Object input) throws IOException {

		Logger log = Logger.getLogger("");
		NosqlClient client = getNosqlClient();
		try {
			List<Map<String, Object>> list = executeSql(client, "select id from JavaQuickstart");
			int max = -1;
			for (Map<String, Object> map : list) {
				int val = (int) map.get("id");
				if (val > max)
					max = val;
			}
			max = max + 1;
			System.out.println("max=" + max);

			@SuppressWarnings("unchecked")
			Map<String, Object> map = (Map<String, Object>) input;
			@SuppressWarnings("unchecked")
			Map<String, Object> cmap = (Map<String, Object>) map.get("data");
			String file = (String) cmap.get("resourceName");
			log.info(file);

			//maxid, filename‚Åinsert
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			String formattedTime = sdf.format(timestamp);
			System.out.println(formattedTime);

			//latlon
			double south = 35.655731;
			double west = 139.698791;
			double north = 35.683144;
			double east = 139.742975;
			double dx = east - west;
			double dy = north - south;
			Random r = new Random();
			double x = r.nextDouble() * dx + west;
			double y = r.nextDouble() * dy + south;
			executeSql(client,
					"insert into JavaQuickstart (id,owner,file,memo,latlon,time) values (" + max
							+ ",'demo','" + file + "'" + ",'demo uploaded!'," + "{'lat': " + y + ", 'lon': " + x
							+ "}," + "'" + formattedTime + "')");

		} catch (Exception e) {
			e.printStackTrace();
			log.warning(e.getMessage());
		}

		return "";

	}

	private List<Map<String, Object>> executeSql(NosqlClient client, String sql) {
		System.out.println(sql);
		QueryDetails qd = QueryDetails.builder()
				.compartmentId("[your-compartment-id]")
				.timeoutInMs(10000)
				.statement(sql)
				.build();
		QueryRequest qreq = QueryRequest.builder()
				.queryDetails(qd)
				.build();
		QueryResponse qres = client.query(qreq);
		QueryResultCollection qrc = qres.getQueryResultCollection();
		List<Map<String, Object>> list = qrc.getItems();
		String str = JsonConverter.objectToJsonBlob(list);
		System.out.println(str);

		return list;
	}

}
