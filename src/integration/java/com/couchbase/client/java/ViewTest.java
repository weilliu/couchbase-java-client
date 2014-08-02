package com.couchbase.client.java;

import static org.junit.Assert.*;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Stale;
import com.couchbase.client.java.query.ViewQuery;
import com.couchbase.client.java.query.ViewResult;
import com.couchbase.client.java.util.ClusterDependentTest;
import com.couchbase.client.java.util.TestProperties;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import rx.Observable;
import rx.Observer;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

public class ViewTest extends ClusterDependentTest {


  @Ignore@Test
  public void shouldQueryView() throws Exception {
      while(true) {
          final CountDownLatch latch = new CountDownLatch(100);
          for (int i = 0; i < 100; i++) {
              bucket().query(ViewQuery.from("foo", "bar").stale(Stale.TRUE)).subscribe(new Observer<ViewResult>() {
                  @Override
                  public void onCompleted() {
                      latch.countDown();
                  }

                  @Override
                  public void onError(Throwable e) {
                      //System.out.println(e);
                  }

                  @Override
                  public void onNext(ViewResult viewRow) {
                      //System.out.println(viewRow.id());
                  }
              });
          }
          latch.await();
      }
  }
  
  
  @Test
  public void shouldViewStaleFalse() throws Exception{
    	
	  	String jdk_version = System.getProperty("java.version");
	  	if (!jdk_version.contains("1.8")){
	  		System.err.println("Needs Java 8 to run shouldViewStaleFalse test");
	  		System.exit(1);
	  	}
	  	else{
	  		System.out.println("Java Version:"+ System.getProperty("java.version"));	
	  	}
	  	
    	
    	//Upsert single line jsonObject to the document id (StaleFalseSync)
    	Document doc = JsonDocument.create("StaleFalse", JsonObject.empty().put("firstname", "John"));
    	bucket().upsert(doc).toBlockingObservable().single();
    	
    	
    	System.out.println("=================== Asynchronous View ==================");
    	Observable<ViewResult> query = bucket().query(ViewQuery.from("users", "by_firstname").stale(Stale.FALSE).limit(10).reduce());
    	query.subscribe(result -> System.out.println(result.toString()));
    	System.out.println("======== Sleep for 2 seconds ========"); //it requires sleep 2 seconds before printing result
    	Thread.sleep(2000);
    	
    	
    	System.out.println("=================== Asynchronous View Again ==================");
    	Observable<ViewResult> query_after2 = bucket().query(ViewQuery.from("users", "by_firstname").stale(Stale.FALSE).limit(10).reduce());
    	query_after2.subscribe(result -> System.out.println(result.toString()));
    	System.out.println("======== Sleep for 2 seconds ========");
    	Thread.sleep(2000);
    	
    	
    	
    	System.out.println("################## Upadte StaleFalse Doc Content  #################"); 	
    	//change the jsonObject Content from John to Johnny for doc id StaleFalse 
    	doc = JsonDocument.create("StaleFalse", JsonObject.empty().put("firstname", "Johnny"));
    	bucket().upsert(doc).toBlockingObservable().single();
    	
    	//delete doc
    	//bucket().remove(doc);
    	
    	
    	System.out.println("=================== Asynchronous View Updated Doc =================");
    	Observable<ViewResult> query_after4 = bucket().query(ViewQuery.from("users", "by_firstname").stale(Stale.FALSE).limit(10).reduce());
    	query_after4.subscribe(result -> System.out.println(result.toString()));
    	System.out.println("======== Sleep for 2 seconds ========");
    	Thread.sleep(2000);
    	
    	  	
    	
    	System.out.println("=================== Asynchronous View Again ==================");
    	Observable<ViewResult> query_after6 = bucket().query(ViewQuery.from("users", "by_firstname").stale(Stale.FALSE).limit(10).reduce());
    	query_after6.subscribe(result -> System.out.println(result.toString()));
    	System.out.println("======== Sleep for 2 seconds ========");
    	Thread.sleep(2000);
  
    	
    	System.exit(0);
    }


  
  	@Test
	public void shouldViewSyncStaleFalse() throws Exception{
  	
	  	String jdk_version = System.getProperty("java.version");
	  	if (!jdk_version.contains("1.8")){
	  		System.err.println("Needs Java 8 to run shouldViewStaleFalse test");
	  		System.exit(1);
	  	}
	  	else{
	  		System.out.println("Java Version:"+ System.getProperty("java.version"));	
	  	}
	  	
  	
	  	//upsert single line jsonObject to the document id (StaleFalseSync)
	  	Document doc = JsonDocument.create("StaleFalseSync", JsonObject.empty().put("firstname", "Michael"));
	  	bucket().upsert(doc).toBlockingObservable().single();
	  	
	  	
	  	System.out.println("=================== Synchronous View ==================");
	  	Iterator<ViewResult> iterator = bucket().query(ViewQuery.from("users", "by_firstname").stale(Stale.FALSE)).toBlockingObservable().getIterator();
	  	while(iterator.hasNext()) {
	  		ViewResult result = iterator.next();
	  		System.out.println(result.toString());
	  		//System.out.println("key:"+result.key()+" value: "+result.value());
	  	}
	  	System.out.println("======== Sleep for 2 seconds ========");
	  	Thread.sleep(2000);
  	
	  	
	  
	  	System.out.println("=================== Synchronous View Again ==================");
	  	Iterator<ViewResult> iterator_after2 = bucket().query(ViewQuery.from("users", "by_firstname").stale(Stale.FALSE)).toBlockingObservable().getIterator();
	  	while(iterator_after2.hasNext()) {
	  		ViewResult result = iterator.next();
	  		System.out.println(result.toString());
	  	}
	  	System.out.println("======== Sleep for 2 seconds ========");
	  	Thread.sleep(2000);
	  	
	  
	  	
	  	
	  	System.out.println("################## Upadte StaleFalse Doc Content  #################"); 	
	  	//change the jsonObject Content from Michael to Mike for doc id StaleFalseSync 
	  	doc = JsonDocument.create("StaleFalseSync", JsonObject.empty().put("firstname", "Mike"));
	  	bucket().upsert(doc).toBlockingObservable().single();
	  	
	  	//delete doc
	  	//bucket().remove(doc);
	  	
	  		
	  	
	  	
	  	System.out.println("=================== Synchronous View Updated Doc =================");
	  	Iterator<ViewResult> iterator_after4 = bucket().query(ViewQuery.from("users", "by_firstname").stale(Stale.FALSE)).toBlockingObservable().getIterator();
	  	while(iterator_after4.hasNext()) {
	  		ViewResult result = iterator.next();
	  		System.out.println(result.toString());
	  	}
	  	System.out.println("======== Sleep for 2 seconds ========");
	  	Thread.sleep(2000);
	  
	  	  	
	  	System.out.println("=================== Synchronous View Again ==================");
	  	Iterator<ViewResult> iterator_after6 = bucket().query(ViewQuery.from("users", "by_firstname").stale(Stale.FALSE)).toBlockingObservable().getIterator();
	  	while(iterator_after6.hasNext()) {
	  		ViewResult result = iterator.next();
	  		System.out.println(result.toString());
	  	}
	  	System.out.println("======== Sleep for 2 seconds ========");
	  	Thread.sleep(2000);
  	

	  	System.exit(0);
  	}
  	
}
