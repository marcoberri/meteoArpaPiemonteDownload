package it.marcoberri.mbmeteo.arpa.download;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class App {

    private static String DOWNLOAD_URL1 = "http://webgis.arpa.piemonte.it/free/rest/services/climatologia-meteorologia-atmosfera/Misure_Temperatura_tempo_reale/MapServer/find?searchText=vercelli&searchFields=&sr=32632&layers=0&f=pjson";

    private static String DOWNLOAD_URL2 = "http://webgis.arpa.piemonte.it/free/rest/services/climatologia-meteorologia-atmosfera/Misure_Pluviometriche_tempo_reale/MapServer/find?searchText=vercelli&searchFields=&sr=32632&layers=0&f=pjson";
  
    		public static void getData(String url, String collName){
    	
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpGet httpget = new HttpGet(url);

            System.out.println("executing request " + httpget.getURI());

            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

                public String handleResponse(
                        final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };
            final String responseBody = httpclient.execute(httpget, responseHandler);
            final Object obj = JSONValue.parse(responseBody);
            final JSONObject jobj = (JSONObject) obj;
            final JSONObject toStoreobj = (JSONObject) ((JSONArray) jobj.get("results")).get(0);
            DBObject dbObject = (DBObject) JSON.parse(toStoreobj.toJSONString());
            System.out.println("----------------------------------------");
            System.out.println(dbObject);
            System.out.println("----------------------------------------");
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            DB db = mongoClient.getDB( "meteoArpa" );
            DBCollection coll = db.getCollection(collName);
            coll.update(dbObject,dbObject,true,false);
            mongoClient.close();
        } catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
            try {
				httpclient.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }

    	
    }
    public static void main(String[] args) throws IOException {
    	getData(DOWNLOAD_URL1, "temperature");
    	getData(DOWNLOAD_URL2, "pluvio");
  
    }
}
