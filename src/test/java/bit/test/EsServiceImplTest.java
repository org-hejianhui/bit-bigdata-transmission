package bit.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import com.alibaba.fastjson.JSON;
import com.bit.bigdata.EsServiceImpl;
import com.bit.bigdata.PosTradeRecord;
import com.bit.bigdata.TradeProduct;

public class EsServiceImplTest extends BasiceTest {
	private EsServiceImpl storeService;

	@Before
	public void setUp() throws Exception {
		storeService = new EsServiceImpl(getBean(ElasticsearchTemplate.class));
	}

	private PosTradeRecord mockData() {
		PosTradeRecord node = new PosTradeRecord();
		node.setOrderId("订单ID");
		node.setOrderMoney(1578120l);
		node.setPosNumber(45);
		node.setShopName("万达广场店");
		node.setTradeDate(System.currentTimeMillis());
		List<TradeProduct> products = new ArrayList<>();
		products.add(new TradeProduct(12, "红牛", 2, 600));
		products.add(new TradeProduct(45178, "益达口香糖", 200, 600));
		node.setProducts(products);
		return node;
	}
	public static void main(String[] args) {
		System.out.println(JSON.toJSONString(new EsServiceImplTest().mockData()));
	}
	int count = 0;
	
	@Test
	public void putTest() throws InterruptedException {
		
		// AbortPolicy 异常终止
		// DiscardPolicy 直接丢弃
		ExecutorService d = 
				new ThreadPoolExecutor(30, Integer.MAX_VALUE, 60L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(), Executors.defaultThreadFactory(),
				new DiscardPolicy());
		
		count = 0;
		long begin = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			final int taskId = i;
			d.execute(new Runnable() {
				@Override
				public void run() {
					String nodeJson = JSON.toJSONString(mockData());
					storeService.putData(nodeJson);
					count++;
					System.out.println(String.format("执行成功：当前数 %s,任务id %s", count, taskId));
				}
			});
		}
//		while (count < 9999) {
//			Thread.sleep(10);
//		}
		System.out.println("用时:" + (System.currentTimeMillis() - begin));
	}
}
