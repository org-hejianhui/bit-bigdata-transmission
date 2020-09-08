package com.bit.bigdata;

import java.util.logging.Logger;

import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;

public class EsServiceImpl {
	private ElasticsearchTemplate esTemplate;
	Logger logger = Logger.getLogger("EsServiceImpl");
	private volatile long count = 0;
	private volatile long fails = 0;
	private volatile long succeeds = 0;

	public EsServiceImpl(ElasticsearchTemplate esTemplate) {
		this.esTemplate = esTemplate;
	}
	public EsServiceImpl() {
	}

	public void putData(String nodeJson) {
		try {
			IndexQuery query = new IndexQueryBuilder()
					.withIndexName("bit_test")
					.withType("PosTradeRecord")
					.withSource(nodeJson).build();
			//esTemplate.index(query);
			succeeds++;
		} catch (Exception e) {
			e.printStackTrace();
			fails++;
		} finally {
			count++;
		}
	}

	public long getCount() {
		return count;
	}

	public long getFails() {
		return fails;
	}

	public long getSucceeds() {
		return succeeds;
	}
	
	
}
