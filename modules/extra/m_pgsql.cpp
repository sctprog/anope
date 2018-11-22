/*
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

/* RequiredLibraries: pq */

#include <string>
#include <sstream>
#include <thread>

#include "modules/postgres.h"
#include "modules/sql.h"

/**
 * @brief Other objects in the postgres module need to be able to find the thread manager
 * I tried to put this in the ModuleHandler class but was having linking errors.
 * Either it can't be done or, more likely, I just don't know how.
 */
namespace PG { static ModuleHandler *ModuleObject; }

// Constructor
PG::QueryRequest::QueryRequest(Service *handler, SQL::Interface *iface, const SQL::Query &quer)
{
	sqlHandler = handler;
	sqlInterface = iface;
	query = quer;
}

// Constructor
// See header file for reasoning behind 2 result classes
PG::QueryResult::QueryResult(SQL::Interface *iface, SQL::Result &res)
{
	sqlInterface = iface;
	result = res;
}

// Constructor
PG::Result::Result(
		unsigned int insId,
		const SQL::Query &queryStr,
		const Anope::string &finalQuery,
		PGresult *resObj)
	: SQL::Result(insId, queryStr, finalQuery)
{
	libpqResultObj = resObj;

	// Prevent segfaults
	if (libpqResultObj == nullptr)
		return;
	Log(LOG_DEBUG) << "m_pgsql: Processing result from query: " << query.query;

	// Look for the position of the word 'INSERT'
	bool isInsert = query.query.find("INSERT", 0) == 0;

	int num_fields = PQnfields(libpqResultObj);
	int num_rows = PQntuples(libpqResultObj);

	// Go through each row of the results and build a table of the result set
	for (int row = 0; row < num_rows; row++)
	{
		std::map<Anope::string, Anope::string> items;

		// each column
		for (int col = 0; col < num_fields; col++)
		{
			Anope::string column = PQfname(libpqResultObj, col);
			Anope::string data = PQgetvalue(libpqResultObj, row, col);

			items[column] = data;

			// If an insert, we've appended to the query to get the id of the row we just added
			// Grab it now.
			if (isInsert && column == "id")
				id = convertTo<unsigned int>(data.str());
		}

		entries.push_back(items);
	}

	Log(LOG_DEBUG)
		<< "m_pgsql: Completed processing of query result. Rows: " << num_rows << ". Cols: " << num_fields
        << "\n --- Query was: " << finalQuery;

	// Done processing, clean up
	PQclear(libpqResultObj);
	libpqResultObj = nullptr;
}

PG::Result::Result(
		const SQL::Query &queryStr,
		const Anope::string &finalQuery,
		const Anope::string &err)
	: SQL::Result(0, queryStr, finalQuery, err)
{
	libpqResultObj = nullptr;
}

// Destructor
PG::Result::~Result()
{
	if (libpqResultObj != nullptr)
	{
		// Instruct libpq to free the result resources
		PQclear(libpqResultObj);

		libpqResultObj = nullptr;
	}
}

// Constructor
PG::ModuleHandler::ModuleHandler(const Anope::string &modname, const Anope::string &creator)
	: Module(modname, creator, EXTRA | VENDOR)
{
	// Make it known where we are
	PG::ModuleObject = this;

	dispatcher = new Dispatcher();
	dispatcher->Start();
}

// Destructor
PG::ModuleHandler::~ModuleHandler()
{
	// Memory clean up
	for (auto &mod :activeConnections)
		delete mod.second;
	activeConnections.clear();

	//Thread clean up
	dispatcher->SetExitState();
	dispatcher->Wakeup();
	dispatcher->Join();
	delete dispatcher;

	PG::ModuleObject = nullptr;
	Log(LOG_DEBUG) << "m_pgsql: PG::ModuleHandler::~ModuleHandler() exiting";
}

// Handle reload of services configuration or initial startup
void PG::ModuleHandler::OnReload(Configuration::Conf *conf)
{
	Configuration::Block *config = conf->GetModule(this);

	// Remove any existing services in the event this isn't initial startup
	for (auto &iter : activeConnections)
	{
		const Anope::string &cname = iter.first;
		PG::Service *service = iter.second;
		int i;

		// Search for the main config block for this service
		for (i = 0; i < config->CountBlock("pgsql"); ++i)
			if (config->GetBlock("pgsql", i)->Get<const Anope::string>("name", "pgsql/main") == cname)
				break;

		// Delete the object associated with it
		if (i == config->CountBlock("pgsql"))
		{
			Log(LOG_NORMAL, "PgSQL") << "m_pgsql: Removing server connection " << cname;

			delete service;
			activeConnections.erase(cname);
		}
	}

	// Find the definition block for our module
	for (int i = 0; i < config->CountBlock("pgsql"); ++i)
	{
		Configuration::Block *block = config->GetBlock("pgsql", i);
		const Anope::string &serviceName = block->Get<const Anope::string>("name", "pgsql/main");

		if (activeConnections.find(serviceName) == activeConnections.end())
		{
			// Populate the SQLd connection data
			const Anope::string &database = block->Get<const Anope::string>("database", "anope");
			const Anope::string &server = block->Get<const Anope::string>("server", "127.0.0.1");
			const Anope::string &user = block->Get<const Anope::string>("username", "anope");
			const Anope::string &password = block->Get<const Anope::string>("password");
			int port = block->Get<int>("port", "5432");

			// Attempt the connection
			try
			{
				Log(LOG_NORMAL, "PgSQL") << "m_pgsql: Instantiating " << serviceName << " (" << server << ")";
				auto *service = new PG::Service(this, serviceName, database, server, user, password, port);

				// Inform the service manager that we are up
				activeConnections.insert(std::make_pair(serviceName, service));
			}

			catch (const SQL::Exception &ex)
			{
				// Likely the connection failed
				Log(LOG_NORMAL, "PgSQL") << "m_pgsql: " << ex.GetReason();
			}
		}
	}
}

// Handle module unloading and services shut down
void PG::ModuleHandler::OnModuleUnload(User *, Module *module)
{
	QueryRequestsLock.lock();

	/*
	 * Wipe all remaining query requests from the pool
	 * We need to work backwards because we're modifying the container that's being iterated
	 */
	for (auto i = (unsigned)QueryRequests.size(); i > 0; --i)
	{
		PG::QueryRequest &request = QueryRequests[i - 1];

		if (request.sqlInterface && request.sqlInterface->owner == module)
		{
			if (i == 1)
			{
				// I don't know why this is here. Maybe to make sure competing locks have been released???
				// I don't really want to mess with it just in case
				request.sqlHandler->Lock.Lock();
				request.sqlHandler->Lock.Unlock();
			}

			QueryRequests.erase(QueryRequests.begin() + i - 1);
		}
	}

	QueryRequestsLock.unlock();

	/*
	 * Handle any remaining finished SQL requests
	 * This allows us to process any remaining information that needs to be
	 */
	OnNotify();
}

// One or more queries have finished executing
void PG::ModuleHandler::OnNotify()
{
	// Do a copy of the finished requests right now so we don't hold up the dispatcher
	FinishedRequestsLock.lock();
	std::deque<PG::QueryResult> finishedRequests = FinishedRequests;
	FinishedRequests.clear();
	FinishedRequestsLock.unlock();

	// Iterate over the finished requests
	for (auto &result :finishedRequests)
	{
		if (result.sqlInterface == nullptr)
		{
			Log(LOG_DEBUG) << "m_pgsql: PG::ModuleHandler::OnNotify(): sqlInterface is null";
			throw SQL::Exception("PG::ModuleHandler::OnNotify(): sqlInterface is null");
		}

		// Let the calling module know what happened, good or bad.
		if (result.result.GetError().empty())
			result.sqlInterface->OnResult(result.result);

		else
			result.sqlInterface->OnError(result.result);
	}
}

/*
 * Constructor
 * Initialize some variables!
 */
PG::Service::Service(
		Module *modObj,
		const Anope::string &serviceName,
		const Anope::string &db,
		const Anope::string &hostname,
		const Anope::string &username,
		const Anope::string &passwd,
		int portNo)
	: Provider(modObj, serviceName)
{
	database = db;
	server = hostname;
	user = username;
	password = passwd;
	port = portNo;

	sqlConnection = nullptr;

	Connect();
}

// Destructor
PG::Service::~Service()
{
	// Find the module handler
	ModuleHandler *modObj = PG::ModuleObject;

	modObj->QueryRequestsLock.lock();
	Lock.Lock();

	// Close the Postgres connection
	PQfinish(sqlConnection);
	sqlConnection = nullptr;

	// Wipe remaining requests
	for (auto i = modObj->QueryRequests.size(); i > 0; --i)
	{
		PG::QueryRequest &request = modObj->QueryRequests[i - 1];

		if (request.sqlHandler == this)
		{
			if (request.sqlInterface)
				request.sqlInterface->OnError(SQL::Result(0, request.query, "SQL Interface is going away"));
			modObj->QueryRequests.erase(modObj->QueryRequests.begin() + i - 1);
		}
	}

	Lock.Unlock();
	modObj->QueryRequestsLock.unlock();
	Log(LOG_DEBUG) << "m_pgsql: Service destructor exiting.";
}

// Enqueue a query for execution
void PG::Service::Run(SQL::Interface *iface, const SQL::Query &query)
{
	// Find the module handler
	PG::ModuleHandler *modObj = PG::ModuleObject;

	modObj->QueryRequestsLock.lock();
	modObj->QueryRequests.emplace_back(PG::QueryRequest(this, iface, query));
	modObj->QueryRequestsLock.unlock();

	modObj->dispatcher->Wakeup();

	Log(LOG_DEBUG) << "m_pgsql: Query enqueued: " << query.query;
}

// Immediately send a query to the database, return it's results
SQL::Result PG::Service::RunQuery(const SQL::Query &query)
{
	Lock.Lock();

	// Do interpolation
	Anope::string real_query = BuildQuery(query);

	if (CheckConnection())
	{
		Log(LOG_DEBUG) << "m_pgsql: Sending query to database: " << real_query;

		PGresult *res = PQexec(sqlConnection, real_query.c_str());

		if (PQresultStatus(res) == PGRES_TUPLES_OK // We got results back with our query
		 || PQresultStatus(res) == PGRES_COMMAND_OK) // We got no results back with our query
		{
			Lock.Unlock();
			return PG::Result(0, query, real_query, res);
		}

		Log(LOG_DEBUG) << "m_pgsql: Query failure. Message returned was: " << PQerrorMessage(sqlConnection);
		Log(LOG_DEBUG) << "m_pgsql: Query was: " << query.query;
		PQclear(res);
	}

	Anope::string error = PQerrorMessage(sqlConnection);
	Lock.Unlock();
	return PG::Result(query, real_query, error);
}

// Ensure the DB side table is on par, insert or alter as necessary
std::vector<SQL::Query> PG::Service::CreateTable(const Anope::string &table, const SQL::Data &data)
{
	std::vector<SQL::Query> queries;
	std::set<Anope::string> &known_cols = active_schema[table];

	// Let's see if the schema is in the database already.
	if (known_cols.empty())
	{
		Log(LOG_DEBUG) << "m_pgsql: Fetching columns for " << table;

		SQL::Result columns = RunQuery("SHOW COLUMNS FROM `" + table + "`");
		for (int i = 0; i < columns.Rows(); ++i)
		{
			const Anope::string &column = columns.Get(i, "Field");

			Log(LOG_DEBUG) << "m_pgsql: Column #" << i << " for " << table << ": " << column;
			known_cols.insert(column);
		}
	}

	// If the table isn't in the database at all, add it in
	if (known_cols.empty())
	{
		// Start with the basics
		Anope::string query_text =
			"CREATE TABLE '" + table
			+ "'('id' int(10) unsigned NOT NULL AUTO_INCREMENT,"
			+ " 'timestamp' timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";

		// Add then the rest ..
		for (auto &it : data.data)
		{
			known_cols.insert(it.first);

			query_text += ", '" + it.first + "' ";

			if (data.GetType(it.first) == Serialize::Data::DT_INT)
				query_text += "int(11)";
			else
				query_text += "text";
		}

		query_text += ", PRIMARY KEY ('id'), KEY 'timestamp_idx' ('timestamp'))";
		queries.emplace_back(query_text);
	}

	// Ensure the existing table in SQL isn't missing any columns
	else
	{
		for (auto &it :data.data)
		{
			if (known_cols.count(it.first) > 0)
				continue;

			known_cols.insert(it.first);

			// Add it in
			Anope::string query_text = "ALTER TABLE '" + table + "' ADD '" + it.first + "' ";
			if (data.GetType(it.first) == Serialize::Data::DT_INT)
				query_text += "int(11)";
			else
				query_text += "text";

			queries.emplace_back(query_text);
		}
	}

	return queries;
}

// Generate SQL for an insert statement
SQL::Query PG::Service::BuildInsert(const Anope::string &table, unsigned int id, SQL::Data &data)
{
	// First off we are building the interpolation string
	std::stringstream query_text;

	// Only insert into columns in the data set
	const std::set<Anope::string> &known_cols = active_schema[table];
	for (auto &column : known_cols)
		if (column != "id" && column != "timestamp" && data.data.count(column) == 0)
			data[column] << "";

	query_text << "INSERT INTO '" << table << "' ('id'";

	// Add in column names
	for (auto &column :data.data)
		query_text << ",'" << column.first << '\'';
	query_text << ") VALUES (" << stringify(id);

	// Add in values
	for (auto &value :data.data)
		query_text << ",@" + value.first + '@';
	query_text << ") ON DUPLICATE KEY UPDATE ";

	// If row already exists, update instead
	for (auto &value :data.data)
		query_text << '\'' << value.first << "'=VALUES('" << value.first << ".),";
	query_text.seekp(-1, std::stringstream::end); // back up a smidge to overwrite that trailing comma

	// Postgres does not return row insert IDs on success so we must append this here.
	// Requires PostgresSQL 8.2 or higher
	query_text << " RETURNING id";

	// Interpolation string is now complete.
	// Now populate the column:value map
	SQL::Query query(query_text.str());
	for (auto &it :data.data)
	{
		Anope::string buf = it.second->str();

		bool escape = true;

		// Handle empty values, pass NULL to the server
		if (buf.empty())
		{
			buf = "NULL";
			escape = false; // NULL and 'NULL' are not the same thing!
		}

		query.SetValue(it.first, buf, escape);
	}

	return query;
}

// Search for a table in the database starting with param
SQL::Query PG::Service::GetTables(const Anope::string &prefix)
{
	return SQL::Query("SHOW TABLES LIKE '" + prefix + "%';");
}

// Connect to the database
void PG::Service::Connect()
{
	std::stringstream connStr;

	/*
	 * Connection string is in standard URI format and should look like:
	 * postgresql://user:pass@host:port/database?option1&option2
	 *
	 * Reference:
	 * https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-CONNSTRING
	 */
	connStr
		<< "postgresql://"
		<< user						<< ':'
		<< password					<< '@'
		<< server					<< ':'
		<< stringify(port)			<< '/'
		<< database					<< '?'
		<< "application_name=Anope"	<< '&'
		<< "sslmode=prefer"			<< '&'
		<< "connect_timeout=1";

	// If this succeeds we are fully connected, on the proper database in a single command.
	sqlConnection = PQconnectdb(connStr.str().c_str());

	if (PQstatus(sqlConnection) != CONNECTION_OK)
		throw SQL::Exception("Unable to connect to PostgreSQL service " + name + ": " + PQerrorMessage(sqlConnection));

	Log(LOG_NORMAL) << "Successfully connected to PostgreSQL service " << name << " at " << server << ":" << port << " (DB: " << database << ", SSL: " << stringify(PQsslInUse(sqlConnection)) << ")";
}

// Verify the connection is still live. Reconnect if not.
bool PG::Service::CheckConnection()
{
	if (sqlConnection == nullptr || PQstatus(sqlConnection) != CONNECTION_OK)
	{
		try
		{
			Connect();
		}
		catch (const SQL::Exception &)
		{
			return false;
		}
	}

	return true;
}

// Safe string escaping specifically for use with postgres
Anope::string PG::Service::Escape(const Anope::string &query)
{
	// Postgres demands double buffer length plus 1 for escape to never overrun
	std::vector<char> buffer(query.length() * 2 + 1);
	int err = 0;

	PQescapeStringConn(sqlConnection, &buffer[0], query.c_str(), query.length(), &err);

	if (err != 0)
		Log(LOG_DEBUG, "PgSQL") << "PgSQL: Escape failure '" << PQerrorMessage(sqlConnection) << "' on string:" << query;

	return &buffer[0];
}

// Generate a safe interpolated query
Anope::string PG::Service::BuildQuery(const SQL::Query &query)
{
	Anope::string real_query = query.query;

	for (auto &param :query.parameters)
	{
		// If the field is numeric we don't need to escape it
		Anope::string searchFor = '@' + param.first + '@';

		Anope::string replaceWith =
			param.second.escape // Escape the string?
			? '\'' + Escape(param.second.data) + '\''
			:  param.second.data;

		real_query = real_query.replace_all_cs(searchFor, replaceWith);
	}

	return real_query;
}

// Generate the SQL for a time_t to UNIX time conversion
Anope::string PG::Service::FromUnixtime(time_t time)
{
	return "FROM_UNIXTIME(" + stringify(time) + ')';
}

/*
 * Main thread loop for query execution
 * Will suspend itself when the queue is empty.
 */
void PG::Dispatcher::Run()
{
	ModuleHandler *modObj = PG::ModuleObject;

	if (modObj == nullptr)
	{
		Log(LOG_DEBUG, "PgSQL") << "PgSQL: Dispatcher::Run(): Module object is null, unable to send queries!";
		return;
	}

	while (!GetExitState())
	{
		// Empty the queue of queries that need to go out
		modObj->QueryRequestsLock.lock();
		if (!modObj->QueryRequests.empty())
		{
			PG::QueryRequest &request = modObj->QueryRequests.front();
			modObj->QueryRequestsLock.unlock();

			SQL::Result result = request.sqlHandler->RunQuery(request.query);

			modObj->QueryRequestsLock.lock();

			if (!modObj->QueryRequests.empty() && modObj->QueryRequests.front().query == request.query)
			{
				if (request.sqlInterface != nullptr)
				{
					modObj->FinishedRequestsLock.lock();
					modObj->FinishedRequests.emplace_back(PG::QueryResult(request.sqlInterface, result));
					modObj->FinishedRequestsLock.unlock();
				}
				modObj->QueryRequests.pop_front();
			}

			modObj->QueryRequestsLock.unlock();
		}

		// Handle all of the responses we just generated
		else
		{
			modObj->FinishedRequestsLock.lock();
			bool waitingResults = !modObj->FinishedRequests.empty();
			modObj->FinishedRequestsLock.unlock();
			modObj->QueryRequestsLock.unlock();

			// Let the module handler know we have some work for it
			if (waitingResults)
				modObj->OnNotify();

			// PG::Service::Run() will wake us up as needed
			Wait();
		}
	}

	Log(LOG_DEBUG) << "m_pgsql: Got thread exit signal in dispatcher.";
}

// Instantiate the module and inform the module manager we're here
MODULE_INIT(PG::ModuleHandler)
