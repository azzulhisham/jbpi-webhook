//
// To run this integration use:
//
// kamel run jbpi_webhook.groovy
//

// camel-k: language=groovy
// camel-k: name=jbpi-webhook

import com.datagrate.messagehistory.DataAnalyzer
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.camel.Exchange
import org.apache.camel.Processor
import org.apache.camel.util.json.JsonArray;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

// activate JETIC.IO Data Analyzer
DataAnalyzer.activate(getContext(), intercept(), onException(Throwable.class))

 from("debezium-sqlserver:dbz-mssql-1?offsetStorageFileName=offset-file-1.dat&databaseHostname=117.53.152.155&databasePort=1433&databaseUser=sa&databasePassword=Enav-DB123Sql456!!!&databaseServerName=enav&databaseDbname=VTS&tableIncludeList=dbo.Events&databaseHistoryFileFilename=history-file-1.dat&internalKeyConverter=org.apache.kafka.connect.json.JsonConverter&internalValueConverter=org.apache.kafka.connect.json.JsonConverter")
	 
	 .routeId('route-1')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            Object data = exchange.getIn().getBody(Object.class);
        
            try {
            	Gson gson = new Gson();
            	String jsonString = gson.toJson(data);
        
            	ObjectMapper mapper = new ObjectMapper();
            	Map<String, Object> map = mapper.readValue(jsonString, Map.class);
        
            	//Map<String, Object> schema = (Map<String, Object>) map.get("schema");
            	ArrayList<Object> values = (ArrayList<Object>) map.get("values");
        
            	int eventId = (int)values.get(0);
            	int alarmId = (int)values.get(3);
            	int mmsi = (int)values.get(5);
            	int zoneId = (int)values.get(19);
            	String shipName = (String)values.get(6);
            	String callSign = (String)values.get(7);
            	int imo = (int)values.get(36);
            	long timeStamp = (long)values.get(1);
        
            	Date date = new Date(timeStamp);
            	Instant instant = date.toInstant();
            	String resp = "";
            	Boolean photoSvcErr = true;
        
            	if(alarmId == 485 || alarmId == 486 || alarmId == 487 || alarmId == 488) {
            		//get ship photo
        
            		// Load the truststore
            		KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            		trustStore.load(new FileInputStream("/etc/camel/resources/vts-photosvc"), "vts-photosvc".toCharArray());
        
            		// Create key manager
            		KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            		keyManagerFactory.init(trustStore, "vts-photosvc".toCharArray());
        
            		// Create a TrustManagerFactory and initialize it with the truststore
            		TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            		trustManagerFactory.init(trustStore);
        
            		// Create an SSLContext and initialize it with the TrustManager
            		SSLContext sslContext = SSLContext.getInstance("TLS");
            		sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
        
            		// Set default SSLContext
            		SSLContext.setDefault(sslContext);
        
            		URL url = new URL("https://photosvc.web-vts.com/ShipPhotoService/QueryService.svc/PhotosApi?i=0&mmsi=" + mmsi);
        
            		HttpsURLConnection MyConn = (HttpsURLConnection) url.openConnection();
            		MyConn.setSSLSocketFactory(sslContext.getSocketFactory());;
        
            		// Set the request method to "GET"
            		MyConn.setRequestMethod("GET");
            		MyConn.setDoOutput(true);
            		MyConn.setRequestProperty("Content-Type", "application/json; utf-8");
            		MyConn.setRequestProperty("Accept", "application/json");
            		MyConn.setUseCaches(false);
        
            		int responseCode = MyConn.getResponseCode();
            		//System.out.println("GET Response Code :: " + responseCode);
        
            		if (responseCode == MyConn.HTTP_OK){
            			// Create a reader with the input stream reader.
            			BufferedReader in = new BufferedReader(new InputStreamReader(
            				MyConn.getInputStream()));
            			String inputLine;
        
            			// Create a string buffer
            			StringBuffer response = new StringBuffer();
        
            			// Write each of the input line
            			while ((inputLine = in.readLine()) != null) {
            				response.append(inputLine);
            			}
            			in.close();
        
            			resp = response.toString();
            			resp = resp.substring(1, resp.length() - 1);
            			photoSvcErr = false;
        
            			//System.out.println("Response :: " + response);
            			//exchange.getIn().setBody(resp);
            		}
            		else {
            			// Create a reader with the input stream reader.
            			BufferedReader in = new BufferedReader(new InputStreamReader(
            				MyConn.getInputStream()));
            			String inputLine;
        
            			// Create a string buffer
            			StringBuffer response = new StringBuffer();
        
            			// Write each of the input line
            			while ((inputLine = in.readLine()) != null) {
            				response.append(inputLine);
            			}
            			in.close();
        
            			resp = response.toString();
            			//exchange.getIn().setBody(resp);
            		}
            	}
        
            	JSONObject pl = new JSONObject();
            	pl.put("eventId", eventId);
            	pl.put("alarmId", alarmId);
            	pl.put("mmsi", mmsi);
            	pl.put("zoneId", zoneId);
            	pl.put("shipName", shipName);
            	pl.put("callSign", callSign);
            	pl.put("imo", imo);
            	pl.put("timeStamp", instant);
            	pl.put("photoUrl", resp);
            	pl.put("photoSvcErr", photoSvcErr);
        
            	exchange.getIn().setBody(pl);
        
            	//String body = "event :: " + eventId  + "   -   Zone :: " + zoneId + "   -   Date :: " + instant;
            	//System.out.println(body);
        
            } catch (Exception exception) {
            	String body = "error :: " + exception.getMessage() + "   ::   " + data;
            	System.out.println(body);
            	exchange.getIn().setBody(body);
            }
        
        
        }
    }).id('process-00')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            Object payload = exchange.getIn().getBody();
        
            JSONObject pl = new JSONObject();
            pl = (JSONObject)payload;
        
            String photoUrlString = pl.getString("photoUrl");
            int picId = 0;
            String picUrl ="";
        
            if(photoUrlString != "") {
            	JSONObject json =  new JSONObject(photoUrlString);
            	JSONArray array = json.getJSONArray("items");
        
            	for(int i=0; i<array.length(); i++){
            		JSONObject picInfo = array.getJSONObject(i);
            		int picIdObj = picInfo.getInt("PicID");
        
            		if(picIdObj > picId){
            			picId = picIdObj;
            		}
            	}
        
            	for(int i=0; i<array.length(); i++){
            		JSONObject picInfo = array.getJSONObject(i);
            		int picIdObj = picInfo.getInt("PicID");
        
            		if(picIdObj == picId){
            			picUrl = picInfo.getString("name");
            		}
            	}
            }
        
            pl.put("picUrl", picUrl);
            //System.out.println(pl);
        
            if(picUrl != ""){
        
            	KeyStore keyStore = KeyStore.getInstance("JKS");
            	FileInputStream instream = new FileInputStream(new File("/etc/camel/resources/vts-fugro"));
            	try {
            		keyStore.load(instream, "vts-fugro".toCharArray());
            	} finally {
            		instream.close();
            	}
        
            	// Create the SSL context
            	SSLContext sslContext = SSLContexts.custom()
            				.loadTrustMaterial(keyStore, new TrustSelfSignedStrategy())
            				.build();
        
            	// Create the socket factory
            	SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(sslContext);
        
            	// Create the http client
            	HttpClient httpClient = HttpClients.custom()
            				.setSSLSocketFactory(socketFactory)
            				.build();
        
            	// Make the request		
            	HttpGet httpGet = new HttpGet(picUrl);								
        
            	HttpResponse response = httpClient.execute(httpGet);
            	HttpEntity responseEntity = response.getEntity();
        
            	StatusLine statusLine = response.getStatusLine();
            	int statusCode = statusLine.getStatusCode();
        
            	if(statusCode == 200) {
            		byte[] result = EntityUtils.toByteArray(responseEntity);
            		String encodedImage = Base64.getEncoder().encodeToString(result);	
        
            		pl.put("picBase64", encodedImage);
            	} else {
            		pl.put("picBase64", "");
            	}
            } else {
            	pl.put("picBase64", "");
            }
        
            exchange.getIn().setBody(pl);
        
        }
    }).id('process-01')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            Object payload = exchange.getIn().getBody();
        
            JSONObject pl = new JSONObject();
            pl = (JSONObject)payload;
        
            int eventId = pl.getInt("eventId");
            int alarmId = pl.getInt("alarmId");
            int mmsi = pl.getInt("mmsi");
        
            exchange.getIn().setHeader("_eventId", eventId);
            exchange.getIn().setHeader("_mmsi", mmsi);
        
            if(alarmId == 485 || alarmId == 486 || alarmId == 487 || alarmId == 488) {
        
            	// Load the truststore
            	KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            	trustStore.load(new FileInputStream("/etc/camel/resources/vts-datasvc"), "vts-datasvc".toCharArray());
        
            	// Create key manager
            	KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            	keyManagerFactory.init(trustStore, "vts-datasvc".toCharArray());
        
            	// Create a TrustManagerFactory and initialize it with the truststore
            	TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            	trustManagerFactory.init(trustStore);
        
            	// Create an SSLContext and initialize it with the TrustManager
            	SSLContext sslContext = SSLContext.getInstance("TLS");
            	sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
        
            	// Set default SSLContext
            	SSLContext.setDefault(sslContext);
        
            	URL url = new URL("https://datasvc.web-vts.com/ShipService2/LloydsService.svc/DataApi?i=0&mmsi=" + mmsi);
        
            	HttpsURLConnection MyConn = (HttpsURLConnection) url.openConnection();
            	MyConn.setSSLSocketFactory(sslContext.getSocketFactory());;
        
            	// Set the request method to "GET"
            	MyConn.setRequestMethod("GET");
            	MyConn.setDoOutput(true);
            	MyConn.setRequestProperty("Content-Type", "application/json; utf-8");
            	MyConn.setRequestProperty("Accept", "application/json");
            	MyConn.setUseCaches(false);
        
            	int responseCode = MyConn.getResponseCode();
        
            	if (responseCode == MyConn.HTTP_OK){
            		// Create a reader with the input stream reader.
            		BufferedReader in = new BufferedReader(new InputStreamReader(
            			MyConn.getInputStream()));
            		String inputLine;
        
            		// Create a string buffer
            		StringBuffer response = new StringBuffer();
        
            		// Write each of the input line
            		while ((inputLine = in.readLine()) != null) {
            			response.append(inputLine);
            		}
        
            		in.close();
        
            		String resp = response.toString();
            		resp = resp.substring(1, resp.length() - 1);
        
            		if(resp != "") {
            			JSONObject pl_resp = new JSONObject(resp);
            			JSONArray array = pl_resp.getJSONArray("items");
        
            			for(int i=0; i<array.length(); i++){
            				JSONObject data = array.getJSONObject(i);
            				String label = data.getString("label");
            				String text = data.getString("text");
        
            				if(label.toLowerCase().equals("imo ship no")) {
            					exchange.getIn().setHeader("_imo", text);
            				}
            				if(label.toLowerCase().equals("ship name")) {
            					exchange.getIn().setHeader("_shipname", text);
            				}
            				if(label.toLowerCase().equals("call sign")) {
            					exchange.getIn().setHeader("_callsign", text);
            				}
            				if(label.toLowerCase().equals("official number")) {
            					exchange.getIn().setHeader("_officialnumber", text);
            					pl.put("officialnumber", text);
            				}
            			}
            		}
            		else {
            			exchange.getIn().setHeader("_imo", "");
            			exchange.getIn().setHeader("_shipname", "");
            			exchange.getIn().setHeader("_callsign", "");
            			exchange.getIn().setHeader("_officialnumber", "");
            			pl.put("officialnumber", "");	
            		}
            	}
            	else {
            		// Create a reader with the input stream reader.
            		BufferedReader in = new BufferedReader(new InputStreamReader(
            			MyConn.getInputStream()));
            		String inputLine;
        
            		// Create a string buffer
            		StringBuffer response = new StringBuffer();
        
            		// Write each of the input line
            		while ((inputLine = in.readLine()) != null) {
            			response.append(inputLine);
            		}
        
            		in.close();
        
            		//exchange.getIn().setBody(resp);
            	}
        
            }
        
            exchange.getIn().setBody(pl);
        
        }
    }).id('process-02')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            Object payload = exchange.getIn().getBody();
        
            JSONObject pl = new JSONObject();
            pl = (JSONObject)payload;
        
            int eventId = pl.getInt("eventId");
            int alarmId = pl.getInt("alarmId");
            int mmsi = pl.getInt("mmsi");
        
            if(alarmId == 485 || alarmId == 486 || alarmId == 487 || alarmId == 488) {
        
            	// Providing the website URL
            	URL url = new URL("http://34.124.195.95:8080/jbpi/getEventById?_eventId=" + eventId);
        
            	// Creating an HTTP connection
            	HttpURLConnection MyConn = (HttpURLConnection) url.openConnection();
            	MyConn.setRequestMethod("GET");
            	MyConn.setDoOutput(true);
            	MyConn.setRequestProperty("Content-Type","application/json");
        
            	int responseCode = MyConn.getResponseCode();
        
            	if (responseCode == MyConn.HTTP_OK) {
            		BufferedReader in = new BufferedReader(new InputStreamReader(
            			MyConn.getInputStream()));
            		String inputLine;
        
            		// Create a string buffer
            		StringBuffer response = new StringBuffer();
        
            		// Write each of the input line
            		while ((inputLine = in.readLine()) != null) {
            			response.append(inputLine);
            		}
            		in.close();
        
            		String resp = response.toString();
            		JSONArray array = new JSONArray(resp);
            		JSONObject item = array.getJSONObject(0);
        
            		//payload :: VTMSVesselVisit
            		JSONObject VTMSVesselVisit = new JSONObject();
            		VTMSVesselVisit.put("imo", !item.getString("imo").equals("") ? item.getString("imo") : exchange.getIn().getHeader("_imo").toString());
            		VTMSVesselVisit.put("callsign", !item.getString("callsign").equals("") ? item.getString("callsign") : exchange.getIn().getHeader("_callsign"));
            		VTMSVesselVisit.put("officialnumber", exchange.getIn().getHeader("_officialnumber").toString());
            		VTMSVesselVisit.put("mmsi", !exchange.getIn().getHeader("_mmsi").toString().equals("") ? exchange.getIn().getHeader("_mmsi").toString() : Integer.toString(mmsi));
            		VTMSVesselVisit.put("vesselName", !item.getString("vesselName").equals("") ? item.getString("vesselName") : exchange.getIn().getHeader("_shipname"));
            		VTMSVesselVisit.put("time", item.getString("time"));
            		VTMSVesselVisit.put("visitStatusId", item.getString("visitStatusId"));
            		VTMSVesselVisit.put("zone", item.getString("zone"));
        
            		JSONObject VTMSVesselVisitObj = new JSONObject();
            		VTMSVesselVisitObj.put("VTMSVesselVisit", VTMSVesselVisit)
        
            		pl.put("VTMSVesselVisitObj", VTMSVesselVisitObj);
        
        
            		//payload :: VTMSVesselPhoto
            		JSONObject VTMSVesselPhoto = new JSONObject();
            		VTMSVesselPhoto.put("imo", !item.getString("imo").equals("") ? item.getString("imo") : exchange.getIn().getHeader("_imo").toString());
            		VTMSVesselPhoto.put("callsign", !item.getString("callsign").equals("") ? item.getString("callsign") : exchange.getIn().getHeader("_callsign"));
            		VTMSVesselPhoto.put("officialnumber", exchange.getIn().getHeader("_officialnumber").toString());
            		VTMSVesselPhoto.put("mmsi", !exchange.getIn().getHeader("_mmsi").toString().equals("") ? exchange.getIn().getHeader("_mmsi").toString() : Integer.toString(mmsi));
            		VTMSVesselPhoto.put("vesselName", !item.getString("vesselName").equals("") ? item.getString("vesselName") : exchange.getIn().getHeader("_shipname"));
            		VTMSVesselPhoto.put("time", item.getString("time"));
            		VTMSVesselPhoto.put("photoBase64", pl.getBoolean("photoSvcErr") ? "" : pl.getString("picBase64"));
        
            		JSONObject VTMSVesselPhotoObj = new JSONObject();
            		VTMSVesselPhotoObj.put("VTMSVesselPhoto", VTMSVesselPhoto)
        
            		pl.put("VTMSVesselPhotoObj", VTMSVesselPhotoObj);
        
            		//System.out.println("VTMSVesselPhotoObj :: " + VTMSVesselPhotoObj);
            	}
            	else {
            		// Create a reader with the input stream reader.
            		BufferedReader in = new BufferedReader(new InputStreamReader(
            			MyConn.getInputStream()));
            		String inputLine;
        
            		// Create a string buffer
            		StringBuffer response = new StringBuffer();
        
            		// Write each of the input line
            		while ((inputLine = in.readLine()) != null) {
            			response.append(inputLine);
            		}
            		in.close();
        
            		String resp = response.toString();
        
            		pl.put("VTMSVesselVisitObj", new JSONObject());
            		pl.put("VTMSVesselPhotoObj", new JSONObject());
            	}
            }
        
            exchange.getIn().setBody(pl);
        }
    }).id('process-03')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            Object payload = exchange.getIn().getBody();
        
            JSONObject pl = new JSONObject();
            pl = (JSONObject)payload;
        
            int eventId = pl.getInt("eventId");
            int alarmId = pl.getInt("alarmId");
            int mmsi = pl.getInt("mmsi");
        
            if(alarmId == 485 || alarmId == 486 || alarmId == 487 || alarmId == 488){
        
            	JSONObject VTMSVesselVisit = pl.getJSONObject("VTMSVesselVisitObj");
        
            	if(VTMSVesselVisit.length() != 0){
            		// Providing the website URL
            		URL url = new URL("http://154e-2001-e68-544e-b4e7-e5b1-30fd-aaf8-973.ap.ngrok.io/nifihook");
        
            		// Creating an HTTP connection
            		HttpURLConnection MyConn = (HttpURLConnection) url.openConnection();
            		MyConn.setRequestMethod("POST");
            		MyConn.setDoOutput(true);
            		MyConn.setRequestProperty("Content-Type","application/json");
        
            		byte[] out = VTMSVesselVisit.toString().getBytes(StandardCharsets.UTF_8);
            		OutputStream stream = MyConn.getOutputStream();
            		stream.write(out);
        
            		int responseCode = MyConn.getResponseCode();
        
            		if (responseCode == MyConn.HTTP_CREATED) {
            			BufferedReader in = new BufferedReader(new InputStreamReader(
            				MyConn.getInputStream()));
            			String inputLine;
        
            			// Create a string buffer
            			StringBuffer response = new StringBuffer();
        
            			// Write each of the input line
            			while ((inputLine = in.readLine()) != null) {
            				response.append(inputLine);
            			}
            			in.close();
        
            			String resp = response.toString();
            			System.out.println(resp);
            		}
            	}
            }
        
        
        }
    }).id('process-04')