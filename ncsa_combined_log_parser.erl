% reads in NCSA combined-format log lines like this:
% 192.169.1.1 - - [20/Feb/2012:04:17:29 +0000] "GET /comments/recent HTTP/1.1" 200 377 "http://www.apple.com/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.77 Safari/535.7"
%
-module(ncsa_combined_log_parser).
-export([start/2]).
-export([ready/0]).

start(FilePath, NumProcs) ->
	Pids = create_procs(NumProcs),
	{ok, Fd} = file:open(FilePath, read),
	io:format("Start: ~p - ~p~n", [time(), now()]),
	Before = now(),
	parse(Fd, Pids, 1),
	After = now(),
	io:format("Stop:  ~p - ~p~n", [time(), After]),
	Diff = timer:now_diff(After, Before) / 1000000,
	io:format("Time taken: ~ps~n", [Diff]),
	stop_procs(Pids),
	file:close(Fd).

create_procs(NumProcs) -> create_procs(NumProcs, []).

create_procs(0, Acc) -> Acc;
create_procs(NumProcs, Acc) ->
	create_procs(NumProcs-1, [spawn(?MODULE, ready, [])|Acc]).

stop_procs([]) -> [];
stop_procs([P|Ps]) ->
	P ! stop,
	stop_procs(Ps).

parse(Fd, Pids, Count) when Count > length(Pids) -> parse(Fd, Pids, 1);
parse(Fd, Pids, Count) ->
	case file:read_line(Fd) of
		{ok, Line} ->
			Pid = lists:nth(Count, Pids),
			Pid ! {process_line, list_to_binary(Line), self()},
			parse(Fd, Pids, Count+1);
		eof ->
			ok
	end.

ready() ->
	receive
		{process_line, BLine, _Requestor} ->
		%	io:format("~p --> ~p: ~p~n", [self(), _Requestor,lists:reverse(process_ip(BLine,[]))]),
			_Data = lists:reverse(process_ip(BLine,[])),
			io:format("~p~n", _Data),
			ready();
		stop ->
			ok
	end.

split_line(BLine, Divider) ->
	{Pos, _Len} = binary:match(BLine, Divider),
	DivSize = byte_size(Divider),
	Value = binary:part(BLine,0,Pos),
	Remainder = binary:part(BLine, Pos+DivSize, byte_size(BLine)-(Pos+DivSize)),
	[Value, Remainder].

process_ip(BLine,Results) ->
	[IP, Remainder] = split_line(BLine, <<$ >>), 
	process_client_identity(Remainder, [{ip,IP}|Results]).

process_client_identity(BLine, Results) ->
	[_ClientId, Remainder] = split_line(BLine, <<$ >>),
	process_user_id(Remainder, Results).

process_user_id(BLine, Results) ->
	[_UserId, Remainder] = split_line(BLine, <<$ >>),
	process_date(Remainder, Results).

process_date(BLine, Results) ->
	case BLine of
		<<$[, Day:2/binary, $/, Month:3/binary, $/, Year:4/binary,
		$:, Time:8/binary, $ , _GMTOffset:5/binary, $], $ , Remainder/binary>> ->
			MonthNum = convert_month_to_monthnum(Month),
			SQLDate = <<Year:4/binary,$-,MonthNum:2/binary,$-,Day:2/binary,$ ,Time:8/binary>>,
			process_url(Remainder, [{date,SQLDate}|Results]);
		_ ->
			{error, bad_date}
	end.

convert_month_to_monthnum(Month) when is_binary(Month) ->
	Months = [
		{<<"Jan">>,<<"01">>},{<<"Feb">>,<<"02">>},{<<"Mar">>,<<"03">>},
		{<<"Apr">>,<<"04">>},{<<"May">>,<<"05">>},{<<"Jun">>,<<"06">>},
		{<<"Jul">>,<<"07">>},{<<"Aug">>,<<"08">>},{<<"Sep">>,<<"09">>},
		{<<"Oct">>,<<"10">>},{<<"Nov">>,<<"11">>},{<<"Dec">>,<<"12">>}
	],
	{_M,MonthNum} = lists:keyfind(Month, 1, Months),
	MonthNum.

process_url(BLine, Results) ->
	[Request, Remainder] = split_line(BLine, <<$",$ >>),
	RequestParts = binary:split(Request,<<$ >>, [global]),
	URL = lists:nth(2, RequestParts), % sometimes the load balancer just asks "GET /" without specifying protocol version
	process_status_code(Remainder, [{url,URL}|Results]).

process_status_code(<<StatusCode:3/binary, $ , Remainder/binary>>, Results) ->
	process_response_size(Remainder, [{status_code,StatusCode}|Results]).

process_response_size(BLine, Results) ->
	[ResponseSize, Remainder] = split_line(BLine, <<$ >>),
	process_referer_url(Remainder, [{response_size, ResponseSize}|Results]).

process_referer_url(BLine, Results) ->
	[Referer, Remainder] = split_line(BLine, <<$",$ >>),
	process_user_agent(Remainder, [{referer_url, binary:part(Referer, 1, byte_size(Referer)-1)}|Results]).

process_user_agent(BLine, Results) ->
	[UserAgent, _EndLine] = split_line(BLine, <<$\n>>),
	[{user_agent, binary:part(UserAgent, 1, byte_size(UserAgent)-1)}|Results].






