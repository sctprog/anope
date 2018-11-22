/*
 * (C) 2012-2018 Anope Team
 * Contact us at team@anope.org
 *
 * Please read COPYING and README for further details.
 */

#include <string>
#include <sstream>

#include "account.h"
#include "config.h"
#include "modules/m_sql_authentication.h"

// So the authentication result object can find the module handler
static ModuleSQLAuthentication *me;

// Constructor
SQLAuthenticationResult::SQLAuthenticationResult(User *usr, IdentifyRequest *request)
	: SQL::Interface(me)
{
	user = usr;
	req = request;
	req->Hold(me);
}

// Destructor
SQLAuthenticationResult::~SQLAuthenticationResult()
{
	req->Release(me);
}

/*
 * Callback from the database engine
 * This is executed after our query has been sent and returned and there are no errors with it.
 * There may or may not be rows in the set.
 */
void SQLAuthenticationResult::SQLAuthenticationResult::OnResult(const SQL::Result &result)
{
	BotInfo *NickServ = Config->GetClient("NickServ");

	Log(LOG_DEBUG) << "m_sql_authentication: received an answer from the remote database";

	// On 0 results there simply is no match to the username/password combo
	if (result.Rows() == 0)
	{
		Log(LOG_DEBUG) << "m_sql_authentication: Unsuccessful authentication for " << req->GetAccount();
		delete this;
		return;
	}

	// On more than one match we have a serious security problem.
	// Could be a bad query or a misconfigured/compromised remote database.
	if (result.Rows() != 1)
	{
		Log(LOG_DEBUG)
			<< "m_sql_authentication: Security problem! Expected 1 or 0 row matches for account authentication, got " << result.Rows() << "!\n"
			<< " - User: " << req->GetAccount() << '\n'
			<< " - Query was" << result.finished_query;

		if (user && NickServ)
			user->SendMessage(NickServ, _("Unable to authenticate: duplicate remote matches returned. Please notify services administrator."));

		delete this;
		return;
	}

	Log(LOG_DEBUG) << "m_sql_authentication: Successful authentication for " << req->GetAccount();

	// Grab what we need from the SQL result set
	Anope::string email;
	Anope::string unparsed_nicks;
	try
	{
		email = result.Get(0, "email");

		if (me->nicksInArray && me->populateAll)
			unparsed_nicks = result.Get(0, "nicknames");
	}
	catch (const SQL::Exception &ex)
	{
		if (email.empty())
			Log(LOG_DEBUG) << "m_sql_authentication: Expected email information in query result: " << ex.GetReason();

		else if (unparsed_nicks.empty() && me->nicksInArray && me->populateAll)
			Log(LOG_DEBUG) << "m_sql_authentication: Expected nickname array in result set: " << ex.GetReason();
	}

	// The alias record for the nickname the user is currently using
	NickAlias *authAlias = nullptr;

	// The core record the alias belongs to.
	// We use this to prevent multiple unique accounts for the same person
	NickCore *authCore = nullptr;

	// Which aliases are we going to add to the database?
	std::vector<Anope::string> needAdding;

	// The nick array returns to us from the server looking like:
	// {nick1,nick2,nick3}
	if (me->nicksInArray && me->populateAll)
	{
		// Remove the 2 braces on the outside and any whitespace since we're at it
		unparsed_nicks = unparsed_nicks.trim("\r\t\n {}");

		std::vector<Anope::string> nicknames;
		Anope::string token;

		// Separate the nicknames into a vector.
		// Anope is helpful enough to provide a specialised tokenizer for CSV
		commasepstream css(unparsed_nicks);
		while (css.GetToken(token))
			nicknames.emplace_back(token);

		// Which aliases are in the remote database but not present in services?
		for (auto t_nick :nicknames)
		{
			NickAlias *t_alias = NickAlias::Find(t_nick);

			// Found the core account
			if (t_alias != nullptr)
				authCore = t_alias->nc;

			if (req->GetAccount().lower() == t_nick.lower())
				authAlias = t_alias;

			// We're only adding aliases to this list that are different from the one the user is trying to auth with
			else if (t_alias == nullptr)
				needAdding.emplace_back(t_nick);
		}
	}

	// If we're not looking for an array of nicknames just try to find the services account
	else
		authAlias = NickAlias::Find(req->GetAccount());

	if (authAlias == nullptr)
	{
		// Only create a new core account if none exists
		if (authCore == nullptr)
			authCore = new NickCore(req->GetAccount());

		authAlias = new NickAlias(req->GetAccount(), authCore);

		// Announce nick register to all other modules
		FOREACH_MOD(OnNickRegister, (user, authAlias, ""));
		if (user && NickServ)
			user->SendMessage(NickServ, _("Your account \002%s\002 has been successfully created."), authAlias->nick.c_str());
	}

	// Update email record if it doesn't match our internal database
	if (!email.empty() && email != authAlias->nc->email)
	{
		authAlias->nc->email = email;
		if (user && NickServ)
			user->SendMessage(NickServ, _("Your email has been updated to \002%s\002."), email.c_str());
	}

	/*
	 * If populate all is true and there are missing aliases in the services set they will now be in our vector
	 * Iterate the vector and create those aliases now.
	 */
	Anope::string messageAdded = "The following alias(es) have been added to your account:";
	bool onFirst = true;
	for (auto &nick :needAdding)
	{
		if (authCore != nullptr)
		{
			auto *t_alias = new NickAlias(nick, authCore);
			FOREACH_MOD(OnNickRegister, (user, t_alias, "")); // Announce creation to other modules

			// Add a comma to separate nicknames as necessary
			if (!onFirst)
				messageAdded += ',';

			messageAdded += " \002" + nick + '\002';
			onFirst = false;
		}
	}

	// This will be false if no aliases were created.
	if (!onFirst)
		user->SendMessage(NickServ, messageAdded);

	req->Success(me);
	delete this;
}

/*
 * Callback from the database engine
 * This is executed after our query has been sent and returned and there are errors.
 */
void SQLAuthenticationResult::OnError(const SQL::Result &result)
{
	Log(this->owner) << "m_sql_authentication: Error executing query " << result.GetQuery().query << ": " << result.GetError();
	delete this;
}

// Constructor
ModuleSQLAuthentication::ModuleSQLAuthentication(const Anope::string &modname, const Anope::string &creator)
 : Module(modname, creator, EXTRA | VENDOR)
{
	me = this;
}

// Executed during startup or operserv/reload
void ModuleSQLAuthentication::OnReload(Configuration::Conf *conf)
{
	Configuration::Block *config = conf->GetModule(this);
	this->engine = config->Get<const Anope::string>("engine");
	this->query =  config->Get<const Anope::string>("query");
	this->disable_reason = config->Get<const Anope::string>("disable_reason");
	this->disable_email_reason = config->Get<Anope::string>("disable_email_reason");
	this->nicksInArray = config->Get<bool>("nicknames_array");
	this->populateAll = config->Get<bool>("populate_all");
	this->pushChanges = config->Get<bool>("push_changes");
	this->pushQuery_add = config->Get<Anope::string>("push_query_add");
	this->pushQuery_remove = config->Get<Anope::string>("push_query_remove");
	this->SQL = ServiceReference<SQL::Provider>("SQL::Provider", this->engine);
}

// Executed after any command has been received by services but not yet processed.
EventReturn ModuleSQLAuthentication::OnPreCommand(CommandSource &source, Command *command, std::vector<Anope::string> &params)
{
	if (!this->disable_reason.empty() && (command->name == "nickserv/register" || command->name == "nickserv/group"))
	{
		source.Reply(this->disable_reason);
		return EVENT_STOP;
	}

	if (!this->disable_email_reason.empty() && command->name == "nickserv/set/email")
	{
		source.Reply(this->disable_email_reason);
		return EVENT_STOP;
	}

	return EVENT_CONTINUE;
}

/*
 * Executed if there is an authenticate request for any reason
 *
 * Most commonly it will be a nickserv/identify event but it could also be from the
 * web service, if active.
 */
void ModuleSQLAuthentication::OnCheckAuthentication(User *usr, IdentifyRequest *req)
{
	if (!this->SQL)
	{
		Log(this) << "Unable to find SQL engine";
		return;
	}

	SQL::Query queryData(this->query);
	queryData.SetValue("a", req->GetAccount());
	queryData.SetValue("p", req->GetPassword());
	if (usr)
	{
		queryData.SetValue("n", usr->nick);
		queryData.SetValue("i", usr->ip.addr());
	}

	// Logging in via web interface, it's possible there is no current user record
	else
	{
		queryData.SetValue("n", "");
		queryData.SetValue("i", "");
	}

	this->SQL->Run(new SQLAuthenticationResult(usr, req), queryData);

	Log(LOG_DEBUG) << "m_sql_authentication: Checking authentication for " << req->GetAccount();
}

MODULE_INIT(ModuleSQLAuthentication)
