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

#include <libpq-fe.h>
#include <string>

#include "module.h"
#include "modules/sql.h"

class PgSQLService;

/** A query request
 */
struct QueryRequest
{
    /* The connection to the database */
    PgSQLService *service;

    /* The interface to use once we have the result to send the data back */
    SQL::Interface *sqlinterface;

    /* The actual query */
    SQL::Query query;

    QueryRequest(PgSQLService *s, SQL::Interface *i, const SQL::Query &q) : service(s), sqlinterface(i), query(q) { }
};

/** A query result */
struct QueryResult
{
    /* The interface to send the data back on */
    SQL::Interface *sqlinterface;

    /* The result */
    SQL::Result result;

    QueryResult(SQL::Interface *i, SQL::Result &r) : sqlinterface(i), result(r) { }
};

/** A PgSQL result
 */
class PgSQLResult : public SQL::Result
{
    PGresult *res;

 public:
    PgSQLResult(unsigned int i, const SQL::Query &q, const Anope::string &fq, PGresult *r) : SQL::Result(i, q, fq), res(r)
    {
        if (res == NULL)
            return;

        bool isInsert = query.query.find("INSERT", 0) == 0;

        int num_fields = PQnfields(res);
        int num_rows = PQntuples(res);

        if (num_fields == 0 || num_rows == 0)
            return;

        // each row
        for (int row = 0; row < num_rows; row++)
        {
            std::map<Anope::string, Anope::string> items;

            // each column
            for (int col = 0; col < num_fields; col++)
            {
                Anope::string column = PQfname(res, col);
                Anope::string data = PQgetvalue(res, row, col);

                items[column] = data;

                if (isInsert && column== "id")
                    id = convertTo<unsigned int>(data.str());
            }

            entries.push_back(items);
        }
    }

    PgSQLResult(const SQL::Query &q, const Anope::string &fq, const Anope::string &err) : SQL::Result(0, q, fq, err), res(NULL)
    {
    }

    ~PgSQLResult()
    {
        if (res)
            PQclear(res);
        res = NULL;
    }
};

/** A PgSQL connection, there can be multiple
 */
class PgSQLService : public SQL::Provider
{
private:
    std::map<Anope::string, std::set<Anope::string> > active_schema;

    Anope::string database;
    Anope::string server;
    Anope::string user;
    Anope::string password;
    int port;

    PGconn *sql;

    /** Escape a query.
     * Note the mutex must be held!
     */
    Anope::string Escape(const Anope::string &query);

 public:
    /* Locked by the SQL thread when a query is pending on this database,
     * prevents us from deleting a connection while a query is executing
     * in the thread
     */
    Mutex Lock;

    PgSQLService(Module *o, const Anope::string &n, const Anope::string &d, const Anope::string &s, const Anope::string &u, const Anope::string &p, int po);

    ~PgSQLService();

    void Run(SQL::Interface *i, const SQL::Query &query) anope_override;

    SQL::Result RunQuery(const SQL::Query &query) anope_override;

    std::vector<SQL::Query> CreateTable(const Anope::string &table, const SQL::Data &data) anope_override;

    SQL::Query BuildInsert(const Anope::string &table, unsigned int id, SQL::Data &data) anope_override;

    SQL::Query GetTables(const Anope::string &prefix) anope_override;

    void Connect();

    bool CheckConnection();

    Anope::string BuildQuery(const SQL::Query &q);

    Anope::string FromUnixtime(time_t);
};

/** The SQL thread used to execute queries
 */
class DispatcherThread : public Thread, public Condition
{
 public:
    DispatcherThread() : Thread() { }

    void Run() anope_override;
};

class ModulePgSQL;
static ModulePgSQL *me;
class ModulePgSQL : public Module, public Pipe
{
 private:
    /* SQL connections */
    std::map<Anope::string, PgSQLService *> PgSQLServices;


 public:
    /* Pending query requests */
    std::deque<QueryRequest> QueryRequests;

    /* Pending finished requests with results */
    std::deque<QueryResult> FinishedRequests;

    /* The thread used to execute queries */
    DispatcherThread *DThread;

    ModulePgSQL(const Anope::string &modname, const Anope::string &creator) : Module(modname, creator, EXTRA | VENDOR)
    {
        me = this;

        DThread = new DispatcherThread();
        DThread->Start();
    }

    ~ModulePgSQL()
    {
        for (std::map<Anope::string, PgSQLService *>::iterator it = PgSQLServices.begin(); it != PgSQLServices.end(); ++it)
            delete it->second;
        PgSQLServices.clear();

        DThread->SetExitState();
        DThread->Wakeup();
        DThread->Join();
        delete DThread;
    }

    void OnReload(Configuration::Conf *conf) anope_override
    {
        Configuration::Block *config = conf->GetModule(this);

        for (std::map<Anope::string, PgSQLService *>::iterator it = PgSQLServices.begin(); it != PgSQLServices.end();)
        {
            const Anope::string &cname = it->first;
            PgSQLService *s = it->second;
            int i;

            ++it;

            for (i = 0; i < config->CountBlock("pgsql"); ++i)
                if (config->GetBlock("pgsql", i)->Get<const Anope::string>("name", "pgsql/main") == cname)
                    break;

            if (i == config->CountBlock("pgsql"))
            {
                Log(LOG_NORMAL, "PgSQL") << "PgSQL: Removing server connection " << cname;

                delete s;
                PgSQLServices.erase(cname);
            }
        }

        for (int i = 0; i < config->CountBlock("pgsql"); ++i)
        {
            Configuration::Block *block = config->GetBlock("pgsql", i);
            const Anope::string &connname = block->Get<const Anope::string>("name", "pgsql/main");

            if (PgSQLServices.find(connname) == PgSQLServices.end())
            {
                const Anope::string &database = block->Get<const Anope::string>("database", "anope");
                const Anope::string &server = block->Get<const Anope::string>("server", "127.0.0.1");
                const Anope::string &user = block->Get<const Anope::string>("username", "anope");
                const Anope::string &password = block->Get<const Anope::string>("password");
                int port = block->Get<int>("port", "5432");

                try
                {
                    PgSQLService *ss = new PgSQLService(this, connname, database, server, user, password, port);
                    PgSQLServices.insert(std::make_pair(connname, ss));

                    Log(LOG_NORMAL, "PgSQL") << "PgSQL: Successfully connected to server " << connname << " (" << server << ")";
                }
                catch (const SQL::Exception &ex)
                {
                    Log(LOG_NORMAL, "PgSQL") << "PgSQL: " << ex.GetReason();
                }
            }
        }
    }

    void OnModuleUnload(User *, Module *m) anope_override
    {
        DThread->Lock();

        for (unsigned i = QueryRequests.size(); i > 0; --i)
        {
            QueryRequest &r = QueryRequests[i - 1];

            if (r.sqlinterface && r.sqlinterface->owner == m)
            {
                if (i == 1)
                {
                    r.service->Lock.Lock();
                    r.service->Lock.Unlock();
                }

                QueryRequests.erase(QueryRequests.begin() + i - 1);
            }
        }

        DThread->Unlock();

        OnNotify();
    }

    void OnNotify() anope_override
    {
        DThread->Lock();
        std::deque<QueryResult> finishedRequests = FinishedRequests;
        FinishedRequests.clear();
        DThread->Unlock();

        for (std::deque<QueryResult>::const_iterator it = finishedRequests.begin(), it_end = finishedRequests.end(); it != it_end; ++it)
        {
            const QueryResult &qr = *it;

            if (!qr.sqlinterface)
                throw SQL::Exception("NULL qr.sqlinterface in PgSQLPipe::OnNotify() ?");

            if (qr.result.GetError().empty())
                qr.sqlinterface->OnResult(qr.result);
            else
                qr.sqlinterface->OnError(qr.result);
        }
    }
};

PgSQLService::PgSQLService(Module *o, const Anope::string &n, const Anope::string &d, const Anope::string &s, const Anope::string &u, const Anope::string &p, int po)
: Provider(o, n), database(d), server(s), user(u), password(p), port(po), sql(NULL)
{
    Connect();
}

PgSQLService::~PgSQLService()
{
    me->DThread->Lock();
    Lock.Lock();
    PQfinish(sql);
    sql = NULL;

    for (unsigned i = me->QueryRequests.size(); i > 0; --i)
    {
        QueryRequest &r = me->QueryRequests[i - 1];

        if (r.service == this)
        {
            if (r.sqlinterface)
                r.sqlinterface->OnError(SQL::Result(0, r.query, "SQL Interface is going away"));
            me->QueryRequests.erase(me->QueryRequests.begin() + i - 1);
        }
    }
    Lock.Unlock();
    me->DThread->Unlock();
}

void PgSQLService::Run(SQL::Interface *i, const SQL::Query &query)
{
    me->DThread->Lock();
    me->QueryRequests.push_back(QueryRequest(this, i, query));
    me->DThread->Unlock();
    me->DThread->Wakeup();
}

SQL::Result PgSQLService::RunQuery(const SQL::Query &query)
{
    Lock.Lock();

    Anope::string real_query = BuildQuery(query);

    if (CheckConnection())
    {
        PGresult *res = PQexec(sql, real_query.c_str());

        switch (PQresultStatus(res))
        {

        case PGRES_TUPLES_OK: // We got results back with our query
        case PGRES_COMMAND_OK: //  We got no results back with our query
            Lock.Unlock();
            return PgSQLResult(0, query, real_query, res);

        default: // There was probably a problem.
            PQclear(res);
            break;
        }
    }

    Anope::string error = PQerrorMessage(sql);
    Lock.Unlock();
    return PgSQLResult(query, real_query, error);
}

std::vector<SQL::Query> PgSQLService::CreateTable(const Anope::string &table, const SQL::Data &data)
{
    std::vector<SQL::Query> queries;
    std::set<Anope::string> &known_cols = active_schema[table];

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

    if (known_cols.empty())
    {
        Anope::string query_text = "CREATE TABLE `" + table + "` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
            " `timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
        for (SQL::Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
        {
            known_cols.insert(it->first);

            query_text += ", `" + it->first + "` ";
            if (data.GetType(it->first) == Serialize::Data::DT_INT)
                query_text += "int(11)";
            else
                query_text += "text";
        }
        query_text += ", PRIMARY KEY (`id`), KEY `timestamp_idx` (`timestamp`))";
        queries.push_back(query_text);
    }
    else
        for (SQL::Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
        {
            if (known_cols.count(it->first) > 0)
                continue;

            known_cols.insert(it->first);

            Anope::string query_text = "ALTER TABLE `" + table + "` ADD `" + it->first + "` ";
            if (data.GetType(it->first) == Serialize::Data::DT_INT)
                query_text += "int(11)";
            else
                query_text += "text";

            queries.push_back(query_text);
        }

    return queries;
}

SQL::Query PgSQLService::BuildInsert(const Anope::string &table, unsigned int id, SQL::Data &data)
{
    /* Empty columns not present in the data set */
    const std::set<Anope::string> &known_cols = active_schema[table];
    for (std::set<Anope::string>::iterator it = known_cols.begin(), it_end = known_cols.end(); it != it_end; ++it)
        if (*it != "id" && *it != "timestamp" && data.data.count(*it) == 0)
            data[*it] << "";

    Anope::string query_text = "INSERT INTO `" + table + "` (`id`";

    for (SQL::Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
        query_text += ",`" + it->first + "`";
    query_text += ") VALUES (" + stringify(id);

    for (SQL::Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
        query_text += ",@" + it->first + "@";
    query_text += ") ON DUPLICATE KEY UPDATE ";

    for (SQL::Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
        query_text += "`" + it->first + "`=VALUES(`" + it->first + "`),";
    query_text.erase(query_text.end() - 1);

    // Postgres does not return row insert IDs on success so we must append this here.
    // Will only work on 8.2+
    query_text += " RETURNING id";

    SQL::Query query(query_text);
    for (SQL::Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
    {
        Anope::string buf;
        *it->second >> buf;

        bool escape = true;
        if (buf.empty())
        {
            buf = "NULL";
            escape = false;
        }

        query.SetValue(it->first, buf, escape);
    }

    return query;
}

SQL::Query PgSQLService::GetTables(const Anope::string &prefix)
{
    return SQL::Query("SHOW TABLES LIKE '" + prefix + "%';");
}

void PgSQLService::Connect()
{
    /*
     * Connection string is in standard URI format and should look like:
     * postgresql://user:pass@host:port/database?option1&option2
     *
     * Reference:
     * https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-CONNSTRING
     */
    Anope::string connStr =
        "postgresql://"
        + user + ":"
        + password + "@"
        + server + ":"
        + stringify(port) + "/"
        + database + "?"
        + "application_name=Anope" + "&"
        + "sslmode=prefer" + "&"
        + "connect_timeout=1";

    sql = PQconnectdb(connStr.c_str());

    if (PQstatus(sql) != CONNECTION_OK)
        throw SQL::Exception("Unable to connect to PostgreSQL service " + name + ": " + PQerrorMessage(sql));

    Log(LOG_DEBUG) << "Successfully connected to PostgreSQL service " << name << " at " << server << ":" << port << " (SSL: " << stringify(PQsslInUse(sql)) << ")";
}

bool PgSQLService::CheckConnection()
{
    if (sql == NULL || PQstatus(sql) != CONNECTION_OK)
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

Anope::string PgSQLService::Escape(const Anope::string &query)
{
    std::vector<char> buffer(query.length() * 2 + 1);
    int err = 0;

    PQescapeStringConn(sql, &buffer[0], query.c_str(), query.length(), &err);

    if (err != 0)
        Log(LOG_NORMAL, "PgSQL") << "PgSQL: Escape failure '" << PQerrorMessage(sql) << "' on string:" << query;

    return &buffer[0];
}

Anope::string PgSQLService::BuildQuery(const SQL::Query &q)
{
    Anope::string real_query = q.query;

    for (std::map<Anope::string, SQL::QueryData>::const_iterator it = q.parameters.begin(), it_end = q.parameters.end(); it != it_end; ++it)
        real_query = real_query.replace_all_cs("@" + it->first + "@", (it->second.escape ? ("'" + Escape(it->second.data) + "'") : it->second.data));

    return real_query;
}

Anope::string PgSQLService::FromUnixtime(time_t t)
{
    return "FROM_UNIXTIME(" + stringify(t) + ")";
}

void DispatcherThread::Run()
{
    Lock();

    while (!GetExitState())
    {
        if (!me->QueryRequests.empty())
        {
            QueryRequest &r = me->QueryRequests.front();
            Unlock();

            SQL::Result sresult = r.service->RunQuery(r.query);

            Lock();
            if (!me->QueryRequests.empty() && me->QueryRequests.front().query == r.query)
            {
                if (r.sqlinterface)
                    me->FinishedRequests.push_back(QueryResult(r.sqlinterface, sresult));
                me->QueryRequests.pop_front();
            }
        }
        else
        {
            if (!me->FinishedRequests.empty())
                me->Notify();
            Wait();
        }
    }

    Unlock();
}

MODULE_INIT(ModulePgSQL)
