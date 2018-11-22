/*
 * (C) 2012-2018 Anope Team
 * Contact us at team@anope.org
 *
 * Please read COPYING and README for further details.
 */

//#include "account.h"
#include "modules.h"


//#include "module.h"
#include "modules/sql.h"

#ifndef M_SQL_AUTHENTICATION_H
#define M_SQL_AUTHENTICATION_H

class SQLAuthenticationResult : public SQL::Interface
{
private:
	Reference<User> user;
	IdentifyRequest *req;

 public:
	/**
	 * Constructor
	 * @param usr User record
	 * @param request Identity request object
	 */
	SQLAuthenticationResult(User *usr, IdentifyRequest *request);

	/**
	 * Destructor
	 */
	~SQLAuthenticationResult();

	/**
	 * Executed if the query was successful
	 * @param result Result object containing information about the query sent
	 */
	void OnResult(const SQL::Result &result) override;

	/**
	 * Executed if the database gave an error on the query
	 * @param result Result object containing information about the query sent
	 */
	void OnError(const SQL::Result &result) override;
};

/**
 * Module handler: Configuration handling, core hooks
 */
class ModuleSQLAuthentication : public Module
{
	/**
	 * Name of SQL engine to use for authentication
	 */
	Anope::string engine;

	/**
	 * Query to use for authentication
	 */
	Anope::string query;

	/**
	 * Reason to send to user why they can not register a nickname
	 * nickserv/register will not be disabled if this is empty
	 */
	Anope::string disable_reason;

	/**
	 * Reason to send to the user why they can not set an email address
	 * nickserv/set/email will not be disabled if this is empty
	 */
	Anope::string disable_email_reason;

public:
	/**
	 * Are the DB side nicknames in an array?
	 */
	bool nicksInArray;

	/**
	 * Fetch all nicknames on identify?
	 */
	bool populateAll;

	/**
	 * Push IRC side group changes to the DB?
	 */
	bool pushChanges;

	/**
	 * Query to use on adding an alias
	 */
	Anope::string pushQuery_add;

	/**
	 * Query to use on removing an alias
	 */
	Anope::string pushQuery_remove;

	/**
	 * The database service object to use for queries
	 */
	ServiceReference<SQL::Provider> SQL;

	/**
	 * Constructor
	 * @param modname
	 * @param creator
	 */
	ModuleSQLAuthentication(const Anope::string &modname, const Anope::string &creator);

	/**
	 * Called during startup and configuration reload
	 * @param conf Configuration object
	 */
	void OnReload(Configuration::Conf *conf) override;

	/**
	 * Called before a command is executed
	 * @param source User who sent the command
	 * @param command Command used
	 * @param params Any parameters included
	 * @return EventReturn informing the handler if we're ok with the command going ahead with execution
	 */
	EventReturn OnPreCommand(CommandSource &source, Command *command, std::vector<Anope::string> &params) override;

	/**
	 * Callback, SQL service has returned a result
	 * @param usr Requesting user's record
	 * @param req Request object. See include/account.h
	 */
	void OnCheckAuthentication(User *usr, IdentifyRequest *req) override;
};

#endif // M_SQL_AUTHENTICATION_H
