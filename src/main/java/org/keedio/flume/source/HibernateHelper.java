package org.keedio.flume.source;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.time.DateUtils;
import org.hibernate.CacheMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;

/**
 * Helper class to manage hibernate sessions and perform queries
 * 
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 *
 */
public class HibernateHelper {

	private static final Logger LOG = LoggerFactory
			.getLogger(HibernateHelper.class);

	private static SessionFactory factory;
	private Session session;
	private ServiceRegistry serviceRegistry;
	private Configuration config;
	private SQLSourceHelper sqlSourceHelper;
	public static String dbType = "";
	public final static String DB_TYPE_MYSQL = "mysql";
	public final static String DB_TYPE_SQLSERVER = "sqlserver";

	/**
	 * Constructor to initialize hibernate configuration parameters
	 * @param sqlSourceHelper Contains the configuration parameters from flume config file
	 */
	public HibernateHelper(SQLSourceHelper sqlSourceHelper) {

		this.sqlSourceHelper = sqlSourceHelper;
		Context context = sqlSourceHelper.getContext();

		/* check for mandatory propertis */
		sqlSourceHelper.checkMandatoryProperties();

		Map<String,String> hibernateProperties = context.getSubProperties("hibernate.");
		String conUrl = hibernateProperties.get("connection.url");
		if(conUrl.contains(DB_TYPE_MYSQL)) {
			dbType = DB_TYPE_MYSQL;
		}else if(conUrl.contains(DB_TYPE_SQLSERVER)) {
			dbType = DB_TYPE_SQLSERVER;
		}

		Iterator<Map.Entry<String,String>> it = hibernateProperties.entrySet().iterator();
		
		config = new Configuration();
		Map.Entry<String, String> e;
		
		while (it.hasNext()){
			e = it.next();
			config.setProperty("hibernate." + e.getKey(), e.getValue());
		}


	}

	/**
	 * Connect to database using hibernate
	 */
	public void establishSession() {

		LOG.info("Opening hibernate session");

		serviceRegistry = new StandardServiceRegistryBuilder()
				.applySettings(config.getProperties()).build();
		factory = config.buildSessionFactory(serviceRegistry);
		session = factory.openSession();
		session.setCacheMode(CacheMode.IGNORE);
		
		session.setDefaultReadOnly(sqlSourceHelper.isReadOnlySession());
	}

	/**
	 * Close database connection
	 */
	public void closeSession() {

		LOG.info("Closing hibernate session");
        try {
			session.close();
			factory.close();
		}catch (Throwable ignore) {
        	LOG.warn("close error:", ignore);
		}

	}

	/**
	 * Execute the selection query in the database
	 * @return The query result. Each Object is a cell content. <p>
	 * The cell contents use database types (date,int,string...), 
	 * keep in mind in case of future conversions/castings.
	 * @throws InterruptedException 
	 */

	@SuppressWarnings("unchecked")
	public List<Map<String,Object>> executeQuery(Table table, int retry) throws InterruptedException {

		List<Map<String,Object>> rowsList = new ArrayList() ;
		try {
			Query query;

			if (!session.isOpen()||!session.isConnected()){
				resetConnection();
			}
			query = session.createSQLQuery(sqlSourceHelper.buildQuery(table));

			if("date".equalsIgnoreCase(table.getiFieldType())) {
				SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyyMMddHHmmss");
				SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				try {
					LOG.debug("startFrom:" + table.getStartFrom().toString());
					Date sfDate = dateFormat1.parse(table.getStartFrom()+"");
					Integer pre = table.getPre();

					if(pre != null && table.getPre() > 0) {
						LOG.debug("pre:" + pre);
						sfDate = DateUtils.addSeconds(sfDate, -table.getPre());
					}
					String startFromStr = dateFormat2.format(sfDate);
					LOG.debug("startFromStr:" + startFromStr);
					query = query.setString(0, startFromStr);
				} catch (Exception e) {
					LOG.error("parse error:", e);
					throw new RuntimeException(e);
				}
			}else if("number".equalsIgnoreCase(table.getiFieldType())){
				query = query.setLong(0, table.getStartFrom());
			}else {
				query = query.setString(0, table.getStartFrom().toString());
			}

			if (sqlSourceHelper.getMaxRows() != 0) {
				query = query.setMaxResults(sqlSourceHelper.getMaxRows());
			}
			//LOG.info("startFrom3:" + table.getStartFrom());
			//LOG.info("query:" + query.getQueryString());
			rowsList = query.setFetchSize(sqlSourceHelper.getMaxRows()).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		} catch (Exception e) {
			LOG.error("executeQuery Exception thrown, resetting connection.", e);
			if(retry > 0) {
				LOG.warn("retry:" + retry);
				retry = retry - 1;
				executeQuery(table, retry);
			}
		}

		return rowsList;
	}

	private void resetConnection() throws InterruptedException{
		try {
			session.close();
			factory.close();
		}catch (Exception ignore){
			LOG.warn("close error:", ignore);
		}finally {
			establishSession();
		}


	}
}
