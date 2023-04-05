//
// To run this integration use:
//
// kamel run eip_rest_gateway.groovy
//

// camel-k: language=groovy
// camel-k: name=eip-rest-gateway

// import org.apache.camel.Message
import com.datagrate.messagehistory.DataAnalyzer
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.apache.camel.Exchange
import org.apache.camel.Processor
import org.apache.camel.model.dataformat.JsonLibrary
import org.json.JSONObject;
import org.json.XML;

// activate JETIC.IO Data Analyzer
DataAnalyzer.activate(getContext(), intercept(), onException(Throwable.class))

from('direct://default').routeId('route-1')
    .setBody().simple('This is a EIP Gateway for MMDIS, LRIT and DDMS.').id('setBody-00')

from('direct://ddms').routeId('route-2')
    .to('sql:SELECT sl.ShipListID, sl.IMO, sl.CallSign, sl.Name, sl.ShipType, sl.Dimension_A, sl.Dimension_B, sl.Dimension_C, sl.Dimension_D, sl.MaxDraught, sl.Destination, sl.ETA, sl.EquipTypeID, sl.ESN, sl.DNID, sl.MemberNumber, ddms.*  FROM [VTS].[dbo].[ShipList] sl INNER JOIN [VTS].[dbo].[Draught] ddms on ddms.MMSI = sl.MMSI?batch=false&dataSource=#enavSqlServer&useMessageBodyForSql=false').id('sql-00')
    .marshal().json(JsonLibrary.Jackson).id('toJson-00')

from('direct://lrit get ship info').routeId('route-3')
    .setBody().simple('${header.imo}').id('setBody-01')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            // Providing the website URL
            URL url = new URL("http://lrit.com.my/ASPPositionWebServices/service.asmx");
        
            // Creating an HTTP connection
            HttpURLConnection MyConn = (HttpURLConnection) url.openConnection();
            // Set the request method to "GET"
            //MyConn.setRequestMethod("GET");
            MyConn.setRequestMethod("POST");
            MyConn.setDoOutput(true);
            MyConn.setRequestProperty("Content-Type","text/xml");
            MyConn.setRequestProperty("SOAPAction", "\"http://LRIT.svc/GetShipInfo\"");
            //MyConn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString((userName + ":" + password).getBytes()));
        
            String imo = exchange.getIn().getBody(String.class);
        
            String payload = "<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
                                                        "<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">" +
                                                        "<soap:Body>" +
                                                        "<GetShipInfo xmlns=\"http://LRIT.svc/\">" +
                                                        "<IMONumber>" + imo + "</IMONumber>" +
                                                        "</GetShipInfo>" +
                                                        "</soap:Body>" +
                                                        "</soap:Envelope>";
        
        
            byte[] out = payload.getBytes(StandardCharsets.UTF_8);
            OutputStream stream = MyConn.getOutputStream();
            stream.write(out);
        
            int responseCode = MyConn.getResponseCode();
            //System.out.println("GET Response Code :: " + responseCode);
        
            if (responseCode == MyConn.HTTP_OK) {
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
        
            	String xml = response.toString();
        
            	byte[] encoded = xml.getBytes();
            	JSONObject xmlJSONObj = XML.toJSONObject(new String(encoded));
            	//String json = xmlJSONObj.toString(4);
        
            	JSONObject envelope = xmlJSONObj.getJSONObject("soap:Envelope");
            	JSONObject body = envelope.getJSONObject("soap:Body");
        
            	JSONObject result = new JSONObject();
            	result.put("Body", body);
        
            	exchange.getIn().setBody(result.toString(4));
        
            } else {
            	System.out.println("Error found !!!");
            }
        }
    }).id('process-00')

from('direct://lrit get ships position').routeId('route-4')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            Object startDate = exchange.getIn().getHeader("startDate");
            Object endDate = exchange.getIn().getHeader("endDate");
        
            URL url = new URL("http://lrit.com.my/ASPPositionWebServices/service.asmx");
        
            // Creating an HTTP connection
            HttpURLConnection MyConn = (HttpURLConnection) url.openConnection();
            // Set the request method to "GET"
            //MyConn.setRequestMethod("GET");
            MyConn.setRequestMethod("POST");
            MyConn.setDoOutput(true);
            MyConn.setRequestProperty("Content-Type","text/xml");
            MyConn.setRequestProperty("SOAPAction", "\"http://LRIT.svc/GetPositions\"");
            //MyConn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString((userName + ":" + password).getBytes()));
        
        
            String payload = "<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
            	"<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">" +
            	"<soap:Body>" +
            	"<GetPositions xmlns=\"http://LRIT.svc/\">" +
            	"<from>" + startDate + "</from>" +
            	"<to>" + endDate + "</to>" +
            	"</GetPositions>" +
            	"</soap:Body>" +
            	"</soap:Envelope>";
        
            byte[] out = payload.getBytes(StandardCharsets.UTF_8);
            OutputStream stream = MyConn.getOutputStream();
            stream.write(out);
        
            int responseCode = MyConn.getResponseCode();
            //System.out.println("GET Response Code :: " + responseCode);
        
            if (responseCode == MyConn.HTTP_OK) {
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
        
            	String xml = response.toString();
            	byte[] encoded = xml.getBytes();
            	JSONObject xmlJSONObj = XML.toJSONObject(new String(encoded));
            	String json = xmlJSONObj.toString(4);
        
            	JSONObject envelope = xmlJSONObj.getJSONObject("soap:Envelope");
            	JSONObject body = envelope.getJSONObject("soap:Body");
        
            	JSONObject result = new JSONObject();
            	result.put("Body", body);
        
            	exchange.getIn().setBody(result.toString(4));
            } else {
            	System.out.println("Error found !!!");
            }
        
        }
    }).id('process-01')

from('direct://mmdis get vessel').routeId('route-5')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            Object imo = exchange.getIn().getHeader("imo");
            System.out.println("imo :: " + imo);
            exchange.getIn().setBody("");
        
            URL url = new URL("http://mdm.enav.my:50837/api/MMDIS/vessel/" + imo);
        
            HttpURLConnection MyConn = (HttpURLConnection) url.openConnection();
            // Set the request method to "GET"
            MyConn.setRequestMethod("GET");
            MyConn.setDoOutput(true);
            MyConn.setRequestProperty("Content-Type","application/json");
            //MyConn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString((userName + ":" + password).getBytes()));
        
        
            //String payload = "{" +
            //									"\"vesselId\": " + "\"\"," +
            //									"\"vesselName\": " + "\"\"," +
            //									"\"officialNumber\": " + "\"\"," +
            //									"\"imoNumber\": " + "\"" + imo +  "\"" +
            //									"}";
        
            //byte[] out = payload.getBytes(StandardCharsets.UTF_8);
            //OutputStream stream = MyConn.getOutputStream();
            //stream.write(out);
        
            int responseCode = MyConn.getResponseCode();
            System.out.println("GET Response Code :: " + responseCode);
        
            if (responseCode == MyConn.HTTP_OK) {
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
        
            	exchange.getIn().setBody(resp);
            } else {
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
        
            	exchange.getIn().setBody(resp);
            }
        
        }
    }).id('process-02')

from('direct://jbpi get event by id').routeId('route-6')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            exchange.setProperty("_eventId", exchange.getIn().getHeader("_eventId"));
            //exchange.setProperty("_mmsi", exchange.getIn().getHeader("_mmsi"));
            //exchange.setProperty("_officialnumber", exchange.getIn().getHeader("_officialnumber"));
        }
    }).id('process-03')
    .to("sql:SELECT  IIF(shp.IMO is null OR shp.IMO = 0, '', CONVERT(nvarchar,shp.IMO)) as imo, IIF(shp.CallSign is null, '', CONVERT(nvarchar, shp.CallSign)) as callsign, IIF(shp.Name is null, '', CONVERT(nvarchar, shp.Name)) as vesselName,  Format(evt.TimeStmp, 'dd/MM/yy HH:mm:ss') as time,     CASE        WHEN evt.AlarmID = 485 THEN '0'         WHEN evt.AlarmID = 486 THEN '2'     WHEN evt.AlarmID = 487 THEN '1'     WHEN evt.AlarmID = 488 THEN '7'      END as visitStatusId,      CASE        WHEN evt.AlarmID = 485 THEN ''        WHEN evt.AlarmID = 486 THEN ''    WHEN evt.AlarmID = 487 THEN zon.ZoneName    WHEN evt.AlarmID = 488 THEN zon.ZoneName     END as zone  FROM Events evt  LEFT JOIN Zones zon on zon.ZoneID = evt.ZoneID  LEFT JOIN ShipList shp on shp.MMSI = evt.MMSI  WHERE EventID=:#\${exchangeProperty._eventId} ?batch=false&dataSource=#enavSqlServer&useMessageBodyForSql=false").id('sql-01')
    .marshal().json(JsonLibrary.Jackson).id('toJson-01')

from('direct://mdm get auth token').routeId('route-7')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            Object payload = exchange.getIn().getBody();
        
        
            URL url = new URL("http://mdm.enav.my:50837/api/TokenAuth/Authenticate");
        
            HttpURLConnection MyConn = (HttpURLConnection) url.openConnection();
        
            MyConn.setRequestMethod("POST");
            MyConn.setDoOutput(true);
            MyConn.setRequestProperty("Content-Type","application/json");
            //MyConn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString((userName + ":" + password).getBytes()));
        
            byte[] out = payload.toString().getBytes(StandardCharsets.UTF_8);
            OutputStream stream = MyConn.getOutputStream();
            stream.write(out);
        
            int responseCode = MyConn.getResponseCode();
            System.out.println("GET Response Code :: " + responseCode);
        
            if (responseCode == MyConn.HTTP_OK) {
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
        
            	exchange.getIn().setBody(resp);
            } else {
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
        
            	exchange.getIn().setBody(resp);
            }
        
        }
    }).id('process-04')

from('direct://mdm get vessels').routeId('route-8')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            String authToken = exchange.getIn().getHeader("authorization");
            String queryString = exchange.getIn().getHeader("CamelHttpRawQuery");
        
        
            URL url = new URL("http://mdm.enav.my:50837/api/vessels?" + queryString);
        
            HttpURLConnection MyConn = (HttpURLConnection) url.openConnection();
        
            MyConn.setRequestMethod("GET");
            //MyConn.setDoOutput(true);
            MyConn.setRequestProperty("Content-Type","application/json; charset=utf-8");
            //MyConn.setRequestProperty("Content-Length", "0");
            MyConn.setRequestProperty("Authorization", authToken);
        
            int responseCode = MyConn.getResponseCode();
            //System.out.println("GET Response Code :: " + responseCode);
        
            if (responseCode == MyConn.HTTP_OK) {
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
        
            	exchange.getIn().setBody(resp);
            } else {
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
        
            	exchange.getIn().setBody(resp);
            }
        
        }
    }).id('process-05')

from('direct://mdm get vessels by country code').routeId('route-9')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            String authToken = exchange.getIn().getHeader("authorization");
            String queryString = exchange.getIn().getHeader("CamelHttpRawQuery");
            String countryCode = exchange.getIn().getHeader("countryCode");
        
        
            URL url = new URL("http://mdm.enav.my:50837/api/vesselsByCountry/" + countryCode + "?" + queryString);
        
            HttpURLConnection MyConn = (HttpURLConnection) url.openConnection();
        
            MyConn.setRequestMethod("GET");
            //MyConn.setDoOutput(true);
            MyConn.setRequestProperty("Content-Type","application/json; charset=utf-8");
            //MyConn.setRequestProperty("Content-Length", "0");
            MyConn.setRequestProperty("Authorization", authToken);
        
            int responseCode = MyConn.getResponseCode();
            //System.out.println("GET Response Code :: " + responseCode);
        
            if (responseCode == MyConn.HTTP_OK) {
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
        
            	exchange.getIn().setBody(resp);
            } else {
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
        
            	exchange.getIn().setBody(resp);
            }
        
        }
    }).id('process-06')

from('direct://mdm get vessel by mmsi').routeId('route-10')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            String authToken = exchange.getIn().getHeader("authorization");
            String queryString = exchange.getIn().getHeader("CamelHttpRawQuery");
            String mmsi = exchange.getIn().getHeader("mmsi");
        
        
            URL url = new URL("http://mdm.enav.my:50837/api/vessels/" + mmsi + "?" + queryString);
        
            HttpURLConnection MyConn = (HttpURLConnection) url.openConnection();
        
            MyConn.setRequestMethod("GET");
            //MyConn.setDoOutput(true);
            MyConn.setRequestProperty("Content-Type","application/json; charset=utf-8");
            //MyConn.setRequestProperty("Content-Length", "0");
            MyConn.setRequestProperty("Authorization", authToken);
        
            int responseCode = MyConn.getResponseCode();
            //System.out.println("GET Response Code :: " + responseCode);
        
            if (responseCode == MyConn.HTTP_OK) {
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
        
            	exchange.getIn().setBody(resp);
            } else {
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
        
            	exchange.getIn().setBody(resp);
            }
        
        }
    }).id('process-07')

from('direct://mdm get Methydro').routeId('route-11')
    .process(new Processor() {
        @Override
        void process(Exchange exchange) throws Exception {
            String authToken = exchange.getIn().getHeader("authorization");
            String queryString = exchange.getIn().getHeader("CamelHttpRawQuery");
        
        
            URL url = new URL("http://mdm.enav.my:50837/api/Methydro?" + queryString);
        
            HttpURLConnection MyConn = (HttpURLConnection) url.openConnection();
        
            MyConn.setRequestMethod("GET");
            //MyConn.setDoOutput(true);
            MyConn.setRequestProperty("Content-Type","application/json; charset=utf-8");
            //MyConn.setRequestProperty("Content-Length", "0");
            MyConn.setRequestProperty("Authorization", authToken);
        
            int responseCode = MyConn.getResponseCode();
            //System.out.println("GET Response Code :: " + responseCode);
        
            if (responseCode == MyConn.HTTP_OK) {
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
        
            	exchange.getIn().setBody(resp);
            } else {
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
        
            	exchange.getIn().setBody(resp);
            }
        
        }
    }).id('process-08')