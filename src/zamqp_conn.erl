-module(zamqp_conn).

%% Library functions
-export([open/1, close/1, close_prejudice/1,
         qos/3,
         declare/3, declare/4, delete/3, bind/4,
         consume/2, cancel_ok/3,
         heartbeat/1,
         publish/4, ack/2,
         ingest/2, empty/1,
         passive/1, active_once/1
        ]).

%% Includes
-include_lib("zamqp.hrl").

%% Definitions
-define(TCP_SEND_TIMEOUT, 15000).

-define(TCP_OPTS, [binary,
                   {active, false},
                   {packet, raw},
                   {keepalive, true},
                   {send_timeout, ?TCP_SEND_TIMEOUT},
                   {send_timeout_close, true}]).

-define(START, #{frame := method, class := connection, method := start}).

-define(OPEN_CONN_OK,
        #{frame => method, class => connection,method => open_ok,channel => 0}).
-define(OPEN_CHAN_OK,
        #{frame => method, class => channel, method => open_ok, channel => 1}).
-define(CLOSE_CONN_OK,
        #{frame => method, class => connection,method =>close_ok,channel => 0}).
-define(CLOSE_CHAN_OK,
        #{frame => method, class => channel, method => close_ok, channel => 1}).
-define(BASIC_QOS_OK,
        #{frame => method, class => basic, method => qos_ok, channel => 1}).
-define(DECLARE_EXCHANGE_OK,
        #{frame => method,class => exchange,method => declare_ok,channel => 1}).
-define(DELETE_EXCHANGE_OK,
        #{frame => method,class => exchange, method => delete_ok,channel => 1}).
-define(BIND_OK,
        #{frame => method,class => queue, method => bind_ok,channel => 1}).

-define(WAIT, 5000).

%% Records
-record(state, {socket,
                frame_max,
                heartbeat,
                buff = <<>>,
                limit = erlang:system_time(milli_seconds) + ?WAIT}).

%% Type
-type connection() :: #state{}.

%% ===================================================================
%% Library functions.
%% ===================================================================

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec open(map()) -> {ok, integer(), connection()}.
%%--------------------------------------------------------------------
open(#{user := User, pass := Pass, host := Host, port := Port}) ->
    State = #state{},
    {ok, Sock} = gen_tcp:connect(Host, Port, ?TCP_OPTS, remain(State)),
    try do_open(User, Pass, State#state{socket = Sock})
    catch ?WITH_STACKTRACE(_, Reason, Stacktrace)
            ?WARNING("Failed to open connection ~p:~p@~p:~p ~p:~p",
                     [User, Pass, Host, Port, Reason, Stacktrace]),
            gen_tcp:close(Sock),
            erlang:error(Reason)
    end.

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec close(connection()) -> ok.
%%--------------------------------------------------------------------
close(Con = #state{socket = Sock}) ->
    try do_close(Con) catch _:_ -> ok end,
    gen_tcp:close(Sock) .

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%  No fuzz, now muzz just kill it.
%% @end
%%--------------------------------------------------------------------
-spec close_prejudice(connection()) -> ok.
%%--------------------------------------------------------------------
close_prejudice(#state{socket = Sock}) -> gen_tcp:close(Sock).

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec qos(integer(), boolean(), connection()) -> {ok, connection()}.
%%--------------------------------------------------------------------
qos(PrefetchCount, Global, Con) ->
    Con1 = send_recv(basic,
                     qos,
                     #{channel => 1,
                       prefetch_count => PrefetchCount,
                       global => Global},
                     ?BASIC_QOS_OK,
                     limit(Con)),
    {ok, Con1}.

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec declare(atom(), atom() | binary(), connection()) -> {ok, connection()}.
%%--------------------------------------------------------------------
declare(Type, Name, Con) when is_atom(Name) ->
    declare(Type, atom_to_binary(Name, utf8), Con);
declare(exchange, Name, Con) ->
    Con1 = send_recv(exchange,
                     declare,
                     #{channel => 1, exchange => Name, type => <<"topic">>},
                     ?DECLARE_EXCHANGE_OK,
                     limit(Con)),
    {ok, Con1};
declare(queue, Name, Con) ->
    declare(queue, Name, #{}, Con).

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec declare(atom(), atom() | binary(), map(), connection()) ->
                     {ok, connection()}.
%%--------------------------------------------------------------------
declare(Type, Name, Args, Con) when is_atom(Name) ->
    declare(Type, atom_to_binary(Name, utf8), Args, Con);
declare(exchange, Name, Args, Con) ->
    Default = #{channel => 1, exchange => Name, type => <<"topic">>},
    Args0 = maps:merge(Default, Args),
    Con1 = send_recv(exchange,
                     declare,
                     Args0,
                     ?DECLARE_EXCHANGE_OK,
                     limit(Con)),
    {ok, Con1};
declare(queue, Name, Args, Con) ->
    Con1 = limit(Con),
    send(queue, declare, maps:merge(#{channel => 1, queue => Name}, Args),Con1),
    {#{frame := method,
       class := queue,
       method := declare_ok,
       channel := 1,
       queue := Name},
     Con2} = recv(Con1),
    {ok, Con2}.

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec delete(atom(), atom() | binary(), connection()) -> {ok, connection()}.
%%--------------------------------------------------------------------
delete(Type, Name, Con) when is_atom(Name) ->
    delete(Type, atom_to_binary(Name, utf8), Con);
delete(exchange, Name, Con) ->
    Con1 =
        send_recv(exchange,
                  delete,
                  #{channel => 1, exchange => Name},
                  ?DELETE_EXCHANGE_OK,
                  limit(Con)),
    {ok, Con1};
delete(queue, Name, Con) ->
    Con1 = limit(Con),
    send(queue, delete, #{channel => 1, queue => Name}, Con1),
    {#{frame := method,
       class := queue,
       method := delete_ok,
       channel := 1},
     Con2} = recv(Con1),
    {ok, Con2}.

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec bind(atom() | binary(), atom() | binary(), binary(), connection()) ->
                  {ok, connection()}.
%%--------------------------------------------------------------------
bind(Queue, Exchange, Key, Con) when is_atom(Queue) ->
    bind(atom_to_binary(Queue, utf8), Exchange, Key, Con);
bind(Queue, Exchange, Key, Con) when is_atom(Exchange) ->
    bind(Queue, atom_to_binary(Exchange, utf8), Key, Con);
bind(Queue, Exchange, Key, Con) ->
    Con1 = limit(Con),
    Bind = #{channel => 1,
             exchange => Exchange,
             queue => Queue,
             routing_key => Key},
    {ok, send_recv(queue, bind, Bind, ?BIND_OK, Con1)}.

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec consume(atom() | binary(), connection()) -> {ok, binary(), connection()}.
%%--------------------------------------------------------------------
consume(Queue, Con) when is_atom(Queue) ->
    consume(atom_to_binary(Queue, utf8), Con);
consume(Queue, Con) ->
    Con1 = limit(Con),
    send(basic, consume, #{channel => 1, queue => Queue}, Con1),
    {#{frame := method,
       class := basic,
       method := consume_ok,
       channel := 1,
       consumer_tag := Tag},
     Con2} = recv(Con1),
    {ok, Tag, Con2}.

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec cancel_ok(atom(), binary(), connection()) -> {ok, connection()}.
%%--------------------------------------------------------------------
cancel_ok(consumer, Tag, Con) ->
    Con1 = limit(Con),
    send(basic, cancel_ok, #{channel => 1, consumer_tag => Tag}, Con1),
    {ok, Con1}.

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec heartbeat(connection()) -> ok.
%%--------------------------------------------------------------------
heartbeat(Con) -> send(heartbeat, none, limit(Con)).

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec publish(atom() | binary(), binary(), binary(), connection()) -> ok.
%%--------------------------------------------------------------------
publish(Exchange, Key, Body, Con) when is_atom(Exchange) ->
    publish(atom_to_binary(Exchange, utf8), Key, Body, Con);
publish(Exchange, Key, Body, Con = #state{frame_max = FrameMax}) ->
    Con1 = limit(Con),
    Publish = #{channel => 1,
                exchange => Exchange,
                routing_key => Key,
                body => Body},
    send(basic, publish, Publish, Con1),
    Size = iolist_size(Body),
    Header = #{channel => 1, content_class => basic, content_body_size => Size},
    send(content, header, Header, Con1),
    [send(content, body, #{channel => 1, payload => Bit}, Con1) ||
        Bit <- split_body(Body, Size, FrameMax, [])].

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec ack(binary(), connection()) -> ok.
%%--------------------------------------------------------------------
ack(Tag, #state{socket = Sock}) ->
    Args = #{channel => 1, delivery_tag => Tag},
    ok = gen_tcp:send(Sock, zamqp_protocol:encode(basic, ack, Args)).

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec ingest(binary(), connection()) -> {ok, _, connection()}.
%%--------------------------------------------------------------------
ingest(Bin, Con = #state{socket = Sock, buff = Buff}) ->
    Con1  = limit(Con),
    Data = case {Bin, Buff} of
               {_, <<>>} -> Bin;
               {<<>>, _} -> Buff;
               _ -> <<Buff/binary, Bin/binary>>
           end,
    case zamqp_protocol:decode(Data) of
        {ok, Frame = #{frame := heartbeat}, Buff1} ->
            {ok, Frame, Con1#state{buff = Buff1}};
        {ok,
         Frame = #{frame := method,
                   class := basic,
                   method := deliver,
                   channel := 1},
         Buff1} ->
            {Data1, Con2} = ingest_header(Buff1, Con1#state{buff = <<>>}),
            {ok, Frame#{body => Data1}, Con2};
        {ok,
         Frame = #{frame := method,
                   class := basic,
                   method := cancel,
                   channel := 1},
         Buff1} ->
            {ok, Frame, Con1#state{buff = Buff1}};
        {ok,
         Frame = #{frame := method,
                   class := channel,
                   method := close,
                   channel := 1},
         Buff1} ->
            {ok, Frame, Con1#state{buff = Buff1}};
        {more, N} ->
            {ok, Bin1} = gen_tcp:recv(Sock, N, remain(Con1)),
            ingest(Bin1, Con1#state{buff = Data})
    end.

ingest_header(Data, Con = #state{socket = Sock}) ->
    case zamqp_protocol:decode(Data) of
        {ok,
         #{frame := header,
           class := basic,
           channel := 1,
           content_body_size := Size},
         Data1} ->
            ingest_body(Data1, Size, Con, <<>>);
        {more, N} ->
            {ok, Data1} = gen_tcp:recv(Sock, N, remain(Con)),
            ingest_header(<<Data/binary, Data1/binary>>, Con)
    end.

ingest_body(Data, 0, Con, Acc) -> {Acc, Con#state{buff = Data}};
ingest_body(Data, Size, Con = #state{socket = Sock}, Acc) when Size > 0 ->
    case zamqp_protocol:decode(Data) of
        {ok, #{frame := body, channel := 1, payload := Payload}, Data1} ->
            ingest_body(Data1,
                        Size - iolist_size(Payload),
                        Con,
                        <<Acc/binary, Payload/binary>>);
        {more, N} ->
            {ok, Data1} = gen_tcp:recv(Sock, N, remain(Con)),
            ingest_body(<<Data/binary, Data1/binary>>, Size, Con, Acc)
    end.

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec empty(connection()) -> boolean().
%%--------------------------------------------------------------------
empty(#state{buff = <<>>}) -> true;
empty(_) -> false.

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec active_once(connection()) -> ok.
%%--------------------------------------------------------------------
active_once(#state{socket = Sock}) -> ok = inet:setopts(Sock, [{active, once}]).

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec passive(connection()) -> ok.
%%--------------------------------------------------------------------
passive(#state{socket = Sock}) -> ok = inet:setopts(Sock, [{active, false}]).


%% ===================================================================
%% Internal functions.
%% ===================================================================

do_open(User, Pass, State) ->
    send(protocol, header, State),
    {?START, State1} = recv(State),
    Plain = <<0, User/binary, 0, Pass/binary>>,
    Args  = #{response => Plain,
              client_properties => #{consumer_cancel_notify => true}},
    send(connection, start_ok, Args, State1),
    {#{frame := method,
       class := connection,
       method := tune,
       channel_max := ChanMax,
       frame_max := FrameMax,
       heartbeat := HeartBeat},
     State2} = recv(State1),
    HeartBeat1 = case HeartBeat of
                     0 -> 10;
                     _ -> HeartBeat
                 end,
    Tune = #{channel_max => ChanMax,frame_max =>FrameMax,heartbeat=>HeartBeat1},
    send(connection, tune_ok, Tune, State2),
    State3 = send_recv(connection, open, ?OPEN_CONN_OK, State2),
    State4 = send_recv(channel, open, #{channel => 1}, ?OPEN_CHAN_OK, State3),
    {ok,
     HeartBeat1 * 1000,
     State4#state{frame_max = FrameMax, heartbeat = HeartBeat1 * 1000}}.

do_close(Con) ->
    try send_recv(channel, close, #{channel => 1},?CLOSE_CHAN_OK,limit(Con)) of
        Con1 ->
            try send_recv(connection, close, ?CLOSE_CONN_OK, Con1)
            catch  _:_ -> ok
            end
    catch
        _:_ -> ok
    end.


recv(State) -> recv(0, State).

recv(N, State = #state{socket = Sock, buff = Buff}) ->
    {ok, Bin} = gen_tcp:recv(Sock, N, remain(State)),
    Data = case Buff of
               <<>> -> Bin;
               _ -> <<Buff/binary, Bin/binary>>
           end,
    case zamqp_protocol:decode(Data) of
        {ok, Frame, Buff1} -> {Frame, State#state{buff = Buff1}};
        {more, N1} -> recv(N1, State#state{buff = Data})
    end.

send(Class, Method, State) -> send(Class, Method, #{}, State).

send(Class, Method, Args, State = #state{socket = Sock}) ->
    ok = inet:setopts(Sock, [{send_timeout, remain(State)}]),
    ok = gen_tcp:send(Sock, zamqp_protocol:encode(Class,Method,Args)).

send_recv(Class, Method, Response, State) ->
    send(Class, Method, State),
    {Response, State1} = recv(State),
    State1.

send_recv(Class, Method, Args, Response, State) ->
    send(Class, Method, Args, State),
    {Response, State1} = recv(State),
    State1.

limit(Con)  -> Con#state{limit = erlang:system_time(milli_seconds)+?WAIT}.

remain(#state{limit = Limit}) ->
    case Limit - erlang:system_time(milli_seconds) of
        R when R > 0 -> R;
        _ -> erlang:error(timeout)
    end.

split_body(Body, Size, FrameMax, Acc) when Size =< FrameMax ->
    lists:reverse([Body | Acc]);
split_body(Body, Size, FrameMax, Acc) ->
    <<H:FrameMax/bytes, T/binary>> = Body,
    split_body(T, Size - FrameMax, FrameMax, [H | Acc]).
