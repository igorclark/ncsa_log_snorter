-module(log_entry_db).
-compile(export_all).

-include("log_entry.hrl").

-define(IP_TO_ID, "log_entry_ip_to_id").
-define(URL_TO_ID, "log_entry_ip_to_id").

start(DbFilePath) ->
	register(?MODULE, spawn(log_entry_db, init, [DbFilePath])).

stop() ->
	dets:close(?MODULE),
	?MODULE ! stop,
	ok.

init(DbFilePath) ->
	ets:new(?IP_TO_ID, [named_table, bag]),
	ets:new(?URL_TO_ID, [named_table, bag]),
	dets:open_file(?MODULE, [{type,bag}, {file, DbFilePath}, {keypos, #log_entry.id}]),
	ready().

store_log_entry(Entry) ->
	?MODULE ! {store_entry, Entry, self()},
	receive Reply -> Reply end.

ready() ->
	receive
		{store_entry, Entry, From} ->
			From ! store_entry(Entry),
			ready();
		stop ->
			ok
	end.

store_entry(Entry) ->
	case dets:insert(?MODULE, Entry) of
		{error, Reason} ->
			io:format("# Error in module ~p: ~p~n", [?MODULE, Reason]),
			{error, Reason};
		ok ->
			ok
	end.
