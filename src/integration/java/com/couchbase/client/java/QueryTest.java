package com.couchbase.client.java;

import com.couchbase.client.java.util.ClusterDependentTest;
import com.couchbase.client.java.util.TestProperties;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


public class QueryTest extends ClusterDependentTest {
    @Ignore@Test
    public void shouldQueryView() throws Exception {
        System.out.println(bucket().query("select * from default limit 5").toList().toBlocking().single());
    }
}
