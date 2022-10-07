package com.example.fn;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Logger;

import com.fnproject.fn.api.FnConfiguration;
import com.fnproject.fn.api.RuntimeContext;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CreatePreauthenticatedRequestDetails;
import com.oracle.bmc.objectstorage.model.PreauthenticatedRequest;
import com.oracle.bmc.objectstorage.requests.CreatePreauthenticatedRequestRequest;
import com.oracle.bmc.objectstorage.requests.GetNamespaceRequest;
import com.oracle.bmc.objectstorage.responses.CreatePreauthenticatedRequestResponse;
import com.oracle.bmc.objectstorage.responses.GetNamespaceResponse;

public class GetObjectStorageData {

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
	private ObjectStorageClient getClient() throws IOException {

		final ResourcePrincipalAuthenticationDetailsProvider provider = ResourcePrincipalAuthenticationDetailsProvider
				.builder().build();
		final ObjectStorageClient client = new ObjectStorageClient(provider);
		//        nosqlClient.setRegion(regionId);
		client.setRegion(Region.fromRegionId(regionId));
		return client;
	}

	public String handleRequest(String input) throws Exception {

		Logger log = Logger.getLogger("");
		log.info("GetObjectStorageData start: " + input);

		if (input == null || input.equals(""))
			return null;

		ObjectStorageClient client = getClient();

		GetNamespaceResponse namespaceResponse = client.getNamespace(GetNamespaceRequest.builder().build());
		String namespaceName = namespaceResponse.getValue();

		// fetch the file from the object storage
		String bucketName = "[your-backetname]";
		String objectName = input;

		String ret = "";

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.MINUTE, 5);

		CreatePreauthenticatedRequestDetails createPreauthenticatedRequestDetails = CreatePreauthenticatedRequestDetails
				.builder()
				.name("EXAMPLE-name-Value")
				.bucketListingAction(PreauthenticatedRequest.BucketListingAction.ListObjects)
				.objectName(objectName)
				.accessType(CreatePreauthenticatedRequestDetails.AccessType.AnyObjectRead)
				.timeExpires(calendar.getTime())
				.build();

		CreatePreauthenticatedRequestRequest createPreauthenticatedRequestRequest = CreatePreauthenticatedRequestRequest
				.builder()
				.namespaceName(namespaceName)
				.bucketName(bucketName)
				.createPreauthenticatedRequestDetails(createPreauthenticatedRequestDetails)
				.build();

		CreatePreauthenticatedRequestResponse response = client
				.createPreauthenticatedRequest(createPreauthenticatedRequestRequest);
		PreauthenticatedRequest req = response.getPreauthenticatedRequest();
		String url = "https://[your-objectstorage-endpoint]"
				+ req.getAccessUri() + req.getObjectName();
		log.info("url is filled: " + url);
		ret = url;

		//bytes
		//		byte[] bytes = new byte[] {};
		//		try (final InputStream fileStream = getResponse.getInputStream()) {
		//			bytes = fileStream.readAllBytes();
		//			log.info("bytes.length=" + bytes.length / 1024d + "kb");
		//		} // try-with-resources automatically closes fileStream
		//		client.close();

		client.close();
		return ret;

	} // try-with-resources automatically closes fileStream

}
