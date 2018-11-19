﻿/*
 * PostgreSQL wrapper
 *
 * Intended for use with m_sql_authentication, not tested for any other use case.
 *
 * Written for the Fuel Rats, an Elite: Dangerous community
 * Come find us on irc.fuelrats.com
 *
 * Ported from the the m_mysql module written by the Anope team.
 *
 * This file is licensed under the terms of the FSF GPL 2.0
 * https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
 *
 * (c) 2018 Scott MacDonald <scott@sctprog.ca>
 */

#ifndef MODULES_POSTGRES_H
#define MODULES_POSTGRES_H


#include <libpq-fe.h>
#include <string>
#include <unordered_map>

#include "module.h"
#include "modules/sql.h"

namespace PG
{

class Service;

/** A query request
 */
struct QueryRequest
{
	/**
	 * @brief The handler for the database connection
	 */
	Service *sqlHandler;

	/**
	 * @brief The interface to use once we have the result so we can send the data back to the caller.
	 */
	SQL::Interface *sqlInterface;

	/**
	 * @brief The query text
	 */
	SQL::Query query;

	/**
	 * @brief Constructor
	 * @param handler SQL service h andler
	 * @param iface Interface manager
	 * @param quer Query to execute
	 */
	QueryRequest(Service *handler, SQL::Interface *iface, const SQL::Query &quer);
};

/**
 * @brief The results from a query
 * Generated by PG::Service::RunQuery()
 * This needs to exist because we are inheriting our service handler from a higher power
 */
struct QueryResult
{
	/**
	 * @brief result
	 */
	SQL::Result result;

	/**
	 * @brief The interface to send the data back on
	 */
	SQL::Interface *sqlInterface;

	/**
	 * @brief Constructor, passes params to base class
	 * @param iface SQL Interface that created this object
	 * @param result What the server returned
	 */
	QueryResult(SQL::Interface *iface, SQL::Result &res);
};

/**
 * @brief The Result class
 */
class Result : public SQL::Result
{
private:
	/**
	 * @brief The result object allocated by libpq
	 * Must be freed by PQclear()
	 */
	PGresult *libpqResultObj;

public:
	/**
	 * @brief Constructor
	 * @param insId Unique row insert id, if the query was an insert request
	 * @param query The query as was given to our module
	 * @param finalQuery The final processed query sent to Postgres
	 * @param resObj Library given result object
	 */
	Result(unsigned int insId, const SQL::Query &query, const Anope::string &finalQuery, PGresult *resObj);

	/**
	 * @brief Constructor
	 * @param query Query to execute
	 * @param finalQuery The final query that is ready to be sent to Postgres
	 * @param err The error message returned, if any.
	 */
	Result(const SQL::Query &query, const Anope::string &finalQuery, const Anope::string &err);

	/**
	 * @brief Destructor
	 */
	~Result();
}; // PG::Result class


/**
 * @brief Postgres connection manager
 * Handles all communication directly with the sql service and does all final statement preparations including safety.
 * Owns and controls any memory that must be freed after libpq creates it.
 *
 * There may be multiple of these objects.
 */
class Service : public SQL::Provider
{
private:
	/**
	 * @brief Table data
	 */
	std::map<Anope::string, std::set<Anope::string> > active_schema;

	/**
	 * @brief Hostname of the server to connect to
	 * Populated by PG::ModuleHandler::OnReload()
	 * Configured the modules config file.
	 */
	Anope::string server;

	/**
	 * @brief Username to use in PG connection
	 * Populated by PG::ModuleHandler::OnReload()
	 * Configured the modules config file.
	 */
	Anope::string user;

	/**
	 * @brief Password to authenticate to PG with
	 * Populated by PG::ModuleHandler::OnReload()
	 * Configured the modules config file.
	 */
	Anope::string password;

	/**
	 * @brief port Port PG is listening on. Typically 5432
	 * Populated by PG::ModuleHandler::OnReload()
	 * Configured the modules config file.
	 */
	int port;

	/**
	 * @brief Name of database to use
	 * Populated by PG::ModuleHandler::OnReload()
	 * Configured the modules config file.
	 */
	Anope::string database;

	/**
	 * @brief Tracker of current prepared ids for generating unique ones on new statements
	 */
	int currentPrepared;

	/**
	 * @brief Hash of all active prepared statements
	 */
	std::unordered_map<std::string, int> preparedStatements;

	/**
	 * @brief Pointer to the object used by libpq for this connection
	 */
	PGconn *sqlConnection;

	/**
	 * @brief Escape a string for SQL use
	 * @param query The data to escape
	 * @return Escaped string
	 * Calls PQescapeStringConn() on the supplied data. The returned string will be safe for use in a query.
	 */
	Anope::string Escape(const Anope::string &query);

	/**
	 * @brief Generate a safe interpolated query
	 * @param query Query to operate on
	 * @return Query string
	 */
	Anope::string BuildQuery(const SQL::Query &query);

 public:
	/**
	 * Locked by the SQL thread when a query is pending on this connection.
	 * Prevents connection deletion while there is an execution happening.
	 */
	Mutex Lock;

	/**
	 * @brief Constructor. Create a service object and connect to the backend
	 * @param modObj Pointer to the service object
	 * @param serviceName Name of the service, defined in module config file
	 * @param db Database name to connect to
	 * @param hostname Hostname the DB is on
	 * @param username Username to connect with
	 * @param passwd Which password to use
	 * @param portNo Port number to use. Typically 5432.
	 */
	Service(Module *modObj,
			const Anope::string &serviceName,
			const Anope::string &db,
			const Anope::string &hostname,
			const Anope::string &username,
			const Anope::string &passwd,
			int portNo
			);

	/**
	 * @brief Destructor
	 * Remove any queued queries and close the connection
	 */
	~Service() override;

	/**
	 * @brief Queue a query for execution
	 * @param iface Pointer to the SQL interface, stored as PgModule::sqlInterface
	 * @param query Query to be executed
	 */
	void Run(SQL::Interface *iface, const SQL::Query &query) override;

	/**
	 * @brief Attempt to execute a query and verify it was successful.
	 * @param query Query information
	 * @return A prepared result structure
	 * If this query contains any user submitted data, it should be properlly stored in SQL::Query's parameters map.
	 * This method will handle the interpolation in a safe way.
	 *
	 * The parameters are delineated on both sides with an '@'.
	 * If you SQL::Query::SetValue<string>("friend", "John") and set SQL::Query::query to:
	 *   "My friend's name is @friend@."
	 *
	 * After interpolation it will read: My friend's name is John.
	 * Take care with the parameter mappings as they are case sensitive.
	 */
	SQL::Result RunQuery(const SQL::Query &query) override;

	/**
	 * @brief Generate the SQL statements to create or alter a table in the database if necessary
	 * @param table Name of the table
	 * @param data Table columns
	 * @return Vector of the queries needed to bring the table up to snuff. May be empty.
	 */
	std::vector<SQL::Query> CreateTable(const Anope::string &table, const SQL::Data &data) override;

	/**
	 * @brief Generate the Query object for an insert based on specified table and data
	 * @param table Name of the table to insert into
	 * @param id Id number of insert row
	 * @param data Parameterized column data
	 * @return An SQL::Query that can be passed to PG::Service::RunQuery()
	 */
	SQL::Query BuildInsert(const Anope::string &table, unsigned int id, SQL::Data &data) override;

	/**
	 * @brief Generate a query that searches for table names starting with a specified string
	 * @param prefix Search pattern
	 * @return Generated query
	 */
	SQL::Query GetTables(const Anope::string &prefix) override;

	/**
	 * @brief Connect to the Postgres server
	 * All parameters are configured in the modules configuration file.
	 */
	void Connect();

	/**
	 * @brief Verify that the Postgres connection is healthy
	 * @return True if the connection is good or if the connection is able to be reestablished.
	 */
	bool CheckConnection();

	/**
	 * @brief Generate SQL snippet that converts time_t to Unix time
	 * @param time Value to convert
	 * @return Emebeddable SQL string
	 * Convenience method
	 */
	Anope::string FromUnixtime(time_t) override;
}; // PG::Service class

/**
 * @brief The SQL thread used to execute queries
 */
class Dispatcher : public Thread, public Condition
{
 public:
	/**
	 * @brief Constructor
	 */
	Dispatcher() : Thread() { }

	/**
	 * @brief Main loop for the thread that fires off queries as they are enqueued.
	 * This method will idle once its work is done until it is told to Wake()
	 */
	void Run() override;
};

/**
 * @brief Parsing of configuration, thread management
 */
class ModuleHandler : public Module, public Pipe
{
private:
	/**
	 * @brief List of all connections to any postgres server
	 */
	std::map<Anope::string, PG::Service*> activeConnections;

public:
	/**
	 * @brief Pending queries
	 */
	std::deque<PG::QueryRequest> QueryRequests;

	/**
	 * @brief Pending finished requests with results
	 */
	std::deque<PG::QueryResult> FinishedRequests;

	/**
	 * @brief The thread used to execute queries
	 */
	Dispatcher *dispatcher;

	/**
	 * @brief Constructor
	 * @param modname Name of the module
	 * @param creator Who wrote the module
	 */
	ModuleHandler(const Anope::string &modname, const Anope::string &creator);

	/**
	 * @brief Destructor
	 */
	~ModuleHandler() override;

	/**
	* @brief Configuration file read event handler
	* @param conf The configuration object
	*/
	void OnReload(Configuration::Conf *conf) override;

	/**
	* @brief Module unload event handler
	* @param module Pointer to the module object that is unloading
	* Will fire during services shut down or operserv/unload
	*/
	void OnModuleUnload(User *, Module *module) override;

   /**
	* @brief OnNotify event handler
	* This fires when a query has finished executing
	*/
	void OnNotify() override;
}; // PG::ModuleHandler class

} // PG namespace

#endif // MODULES_POSTGRES_H
