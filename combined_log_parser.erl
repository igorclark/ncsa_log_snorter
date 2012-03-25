% reads in NCSA combined-format log lines like this:
% 192.169.1.1 - - [20/Feb/2012:04:17:29 +0000] "GET /comments/recent HTTP/1.1" 200 377 "http://www.apple.com/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.77 Safari/535.7"
%
-module(combined_log_parser).
-export([process_log_entry/1]).

-include("log_entry.hrl").

process_log_entry(Line) ->
	BinaryLine = list_to_binary(Line),
	[IP, Remainder] = split_line(BinaryLine, <<$ >>), 
	process_client_identity(Remainder, #log_entry{ip=IP}).

split_line(BinaryLine, Divider) ->
	{Pos, _Len} = binary:match(BinaryLine, Divider),
	DivSize = byte_size(Divider),
	Value = binary:part(BinaryLine,0,Pos),
	Remainder = binary:part(BinaryLine, Pos+DivSize, byte_size(BinaryLine)-(Pos+DivSize)),
	[Value, Remainder].

process_client_identity(BinaryLine, LogEntry) ->
	[_ClientId, Remainder] = split_line(BinaryLine, <<$ >>),
	process_user_id(Remainder, LogEntry).

process_user_id(BinaryLine, LogEntry) ->
	[_UserId, Remainder] = split_line(BinaryLine, <<$ >>),
	process_date(Remainder, LogEntry).

process_date(BinaryLine, LogEntry) ->
	case BinaryLine of
		<<$[, Day:2/binary, $/, Month:3/binary, $/, Year:4/binary,
		$:, Time:8/binary, $ , _GMTOffset:5/binary, $], $ , Remainder/binary>> ->
			MonthNum = convert_month_to_monthnum(Month),
			SQLDate = <<Year:4/binary,$-,MonthNum:2/binary,$-,Day:2/binary,$ ,Time:8/binary>>,
			process_url(Remainder, LogEntry#log_entry{date=SQLDate});
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

process_url(BinaryLine, LogEntry) ->
	[Request, Remainder] = split_line(BinaryLine, <<$",$ >>),
	RequestParts = binary:split(Request,<<$ >>, [global]),
	URL = lists:nth(2, RequestParts), % sometimes the load balancer just asks "GET /" without specifying protocol version
	process_status_code(Remainder, LogEntry#log_entry{url=URL}).

process_status_code(<<StatusCode:3/binary, $ , Remainder/binary>>, LogEntry) ->
	process_response_size(Remainder, LogEntry#log_entry{status_code=StatusCode}).

process_response_size(BinaryLine, LogEntry) ->
	[ResponseSize, Remainder] = split_line(BinaryLine, <<$ >>),
	process_referer_url(Remainder, LogEntry#log_entry{response_size=ResponseSize}).

process_referer_url(BinaryLine, LogEntry) ->
	[Referer, Remainder] = split_line(BinaryLine, <<$",$ >>),
	process_user_agent(Remainder, LogEntry#log_entry{referer_url=binary:part(Referer, 1, byte_size(Referer)-1)}).

process_user_agent(BinaryLine, LogEntry) ->
	[UserAgent, _EndLine] = split_line(BinaryLine, <<$\n>>),
	LogEntry#log_entry{id={node(),now()},user_agent=binary:part(UserAgent, 1, byte_size(UserAgent)-1)}.






