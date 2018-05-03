package org.keedio.flume.source;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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

		session.close();
		factory.close();
	}

	/**
	 * Execute the selection query in the database
	 * @return The query result. Each Object is a cell content. <p>
	 * The cell contents use database types (date,int,string...), 
	 * keep in mind in case of future conversions/castings.
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String,Object>> executeQuery(Table table) throws InterruptedException {

		List<Map<String,Object>> rowsList = new ArrayList() ;

		Query query;
		
		if (!session.isConnected()){
			resetConnection();
		}
		query = session.createSQLQuery(sqlSourceHelper.buildQuery(table));

		if("date".equalsIgnoreCase(table.getiFieldType())) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
			try {
				LOG.debug("startFrom:" + table.getStartFrom().toString());
				query = query.setDate(0, dateFormat.parse(table.getStartFrom().toString()));
			} catch (ParseException e) {
				LOG.error("parse error:", e);
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
		try {
			rowsList = query.setFetchSize(sqlSourceHelper.getMaxRows()).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		} catch (Exception e) {
			LOG.error("Exception thrown, resetting connection.", e);
			resetConnection();
		}

		return rowsList;
	}

	private void resetConnection() throws InterruptedException{
		session.close();
		factory.close();
		establishSession();
	}
}
