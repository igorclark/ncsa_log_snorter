% starts up NumProc processes, passes lines from FilePath to them in turn
-module(log_snorter).
-export([start/4, stop/0]).
-export([ready/0]).

-include("log_entry.hrl").

start(FilePath, DbPath, ParserModule, NumProcs) ->
	Pids = create_procs(NumProcs),
	{ok, Fd} = file:open(FilePath, read),
	log_entry_db:start(DbPath),
	Before = now(),
	EntryCount = process_file(Fd, ParserModule, Pids, 1),
	After = now(),
	Diff = timer:now_diff(After, Before) / 1000000,
	stop_procs(Pids),
	file:close(Fd),
	io:format("Log snorted, time taken: ~ps~n", [Diff]),
	{entry_count, EntryCount}.

stop() ->
	log_entry_db:stop().

create_procs(NumProcs) -> create_procs(NumProcs, []).

create_procs(0, Acc) -> Acc;
create_procs(NumProcs, Acc) ->
	create_procs(NumProcs-1, [spawn(?MODULE, ready, [])|Acc]).

stop_procs([]) -> [];
stop_procs([P|Ps]) ->
	P ! stop,
	stop_procs(Ps).

process_file(Fd, ParserModule, Pids, PidIndex) ->
	process_file(Fd, ParserModule, Pids, PidIndex, 0, ok).

process_file(Fd, ParserModule, Pids, PidIndex, LineCount, ReadStatus) when PidIndex > length(Pids) ->
	process_file(Fd, ParserModule, Pids, 1, LineCount, ReadStatus);
process_file(_Fd, _ParserModule, _Pids, 1, LineCount, ReadStatus) when ReadStatus == false ->
	LineCount;
process_file(Fd, ParserModule, Pids, PidIndex, LineCount, _ReadStatus) ->
	NewLineCount = case file:read_line(Fd) of
		{ok, Line} ->
			Pid = lists:nth(PidIndex, Pids),
			Pid ! {process_line, ParserModule, Line, self()},
			LineCount+1;
		eof ->
			LineCount
	end,
	NewReadStatus = NewLineCount > LineCount,
	process_file(Fd, ParserModule, Pids, PidIndex+1, NewLineCount, NewReadStatus).

ready() ->
	receive
		{process_line, ParserModule, Line, _From} ->
			Entry = ParserModule:process_log_entry(Line),
			io:format("~p~n", [binary_to_list(Entry#log_entry.ip)]),
			log_entry_db:store_log_entry(Entry),
			ready();
		stop ->
			ok
	end.

