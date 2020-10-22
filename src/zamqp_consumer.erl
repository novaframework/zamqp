-module(zamqp_consumer).

-behaviour(gen_server).

%% Management API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3
        ]).

%% Includes
-include_lib("zamqp.hrl").

%% Defines
-define(CON_INTERVAL, 5000).

%% Records
-record(state, {module,
                arity,
                user,
                pass,
                host,
                port,
                type,
                routing_key,
                exchange,
                queue,
                auto_delete,
                prefetch_count,
                connection,
                heartbeat,
                tag,
                timeout,
                beat}).

%%====================================================================
%% Behaviour callbacks
%%====================================================================
-callback handle(json:json()) -> ok | error.
-callback handle(binary(), json:json()) -> ok | error.

-optional_callbacks([handle/1, handle/2]).

%% ===================================================================
%% Management API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok, Pid}
%% @doc
%%   Starts the cowboy controlling server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(#{}, #{}) -> {ok, pid()}.
%%--------------------------------------------------------------------
start_link(Consumer, Server) ->
    gen_server:start_link(?MODULE, {Consumer, Server}, []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec init({#{}, #{}}) -> {ok, #state{}}.
%%--------------------------------------------------------------------
init({Consumer, Server}) ->
    process_flag(trap_exit, true),
    #{user := User, pass := Pass, host := Host, port := Port} = Server,
    #{module := Module,
      arity := Arity,
      routing_key := RoutingKey,
      exchange := Exchange,
      queue := Queue,
      type := Type} = Consumer,
    self() ! connect,
    PrefetchCount = maps:get(prefetch_count, Consumer, 10),
    {ok,
     #state{module = Module,
            arity = Arity,
            user = User,
            pass = Pass,
            host = Host,
            port = Port,
            type = Type,
            routing_key = RoutingKey,
            queue = Queue,
            auto_delete = maps:get(auto_delete, Consumer, false),
            exchange = Exchange,
            prefetch_count = PrefetchCount
           }}.

%%--------------------------------------------------------------------
-spec handle_call(_, _, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_call(Req, _, State) ->
    ?UNEXPECTED(call, Req),
    {noreply, State}.

%%--------------------------------------------------------------------
-spec handle_cast(_, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_cast(Cast, State) ->
    ?UNEXPECTED(cast, Cast),
    {noreply, State}.

%%--------------------------------------------------------------------
-spec handle_info(_, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_info(connect, State = #state{host=Host,exchange=Exchange,queue=Queue}) ->
    State1 =
        try do_connect(State) of
            {HeartBeat, CTag, Con} ->
                ?INFO("Connected to RabbitMQ host:~p exchange:~p queue:~p",
                      [Host, Exchange, Queue]),
                is_done(Con),
                start_timers(State#state{heartbeat = HeartBeat,
                                         tag = CTag,
                                         connection = Con})
        catch
            ?WITH_STACKTRACE(Class, Reason, Stacktrace)
                #state{user = User, pass = Pass, port = Port} = State,
                ?ERROR("Failed to connect to RabbitMQ ~p:~p (~p:~p): ~p:~p ~p",
                       [User,
                        Pass,
                        Host,
                        Port,
                        Class,
                        Reason,
                        Stacktrace]),
                connect(?CON_INTERVAL, State)
        end,
    {noreply, State1};
handle_info(beat, State) ->
    State1 =
        try beat(State)
        catch ?WITH_STACKTRACE(Class, Reason, Stacktrace)
                #state{host = Host, port = Port, connection = Con} = State,
                close_prejudice(Con),
                ?ERROR("Failed to beat ~p:~p: ~p:~p",
                       [Host, Port, Class, Reason]),
                connect(?CON_INTERVAL, State)
        end,
    {noreply, State1};
handle_info(timeout, State) ->
    #state{connection = Con,
           host = Host,
           port = Port,
           module = Module,
           exchange = Exchange,
           queue = Queue} = State,
    ?ERROR("RabbitMQ timeout ~p:~p ~p,~p,~p",
           [Host, Port, Module, Exchange, Queue]),
    close_prejudice(Con),
    {noreply, connect(immedately, State)};
handle_info({tcp_close, _}, State = #state{host = Host, port = Port}) ->
    ?ERROR("RabbitMQ TCP close ~p:~p", [Host, Port]),
    {noreply, connect(immedately, State)};
handle_info({tcp_error, Sock, Reason}, State) ->
    #state{host = Host, port = Port} = State,
    try gen_tcp:close(Sock) catch ?WITH_STACKTRACE(_, _, _) ok end,
    ?ERROR("RabbitMQ TCP error ~p ~p:~p", [Reason, Host, Port]),
    {noreply, connect(immedately, State)};
handle_info(continue, State) ->
    {noreply, handle_data(<<>>, State)};
handle_info({tcp, _, Bin}, State) ->
    {noreply, handle_data(Bin, State)};
handle_info(Info, State) ->
    ?UNEXPECTED(info, Info),
    {noreply, State}.

%%--------------------------------------------------------------------
-spec terminate(_, #state{}) -> ok.
%%--------------------------------------------------------------------
terminate(_, #state{connection = Con}) -> close(Con).

%%--------------------------------------------------------------------
-spec code_change(_, #state{}, _) -> {ok, #state{}}.
%%--------------------------------------------------------------------
code_change(_, State, _) -> {ok, State}.

%% ===================================================================
%% Internal functions.
%% ===================================================================

connect(immedately, State) ->
    self() ! connect,
    stop_timers(State#state{connection = undefined});
connect(Time, State) ->
    erlang:send_after(Time, self(), connect),
    stop_timers(State#state{connection = undefined}).

do_connect(State = #state{user = User, pass = Pass, host = Host, port=Port}) ->
    Open = #{user => User, pass => Pass, host => Host, port => Port},
    {ok, HeartBeat, Con} = zamqp_conn:open(Open),
    try setup(Con, State) of
        {ok, CTag, Con1} -> {HeartBeat, CTag, Con1}
    catch
        ?WITH_STACKTRACE(Class, Reason, Stacktrace)
            zamqp_conn:close_prejudice(Con),
            erlang:Class(Reason)
    end.

setup(Con, State) ->
    #state{routing_key = RoutingKey,
           exchange = Exchange,
           queue = Queue,
           auto_delete = Auto,
           prefetch_count = PrefetchCount} = State,
    {ok, Con1} = zamqp_conn:qos(PrefetchCount, true, Con),
    {ok, Con2} = zamqp_conn:declare(exchange, Exchange, Con1),
    {ok, Con3} = zamqp_conn:declare(queue, Queue, #{auto_delete => Auto}, Con2),
    {ok, Con4} = bind(Queue, Exchange, RoutingKey, Con3),
    {ok, _, _} = zamqp_conn:consume(Queue, Con4).

bind(_, _, [], Con) -> {ok, Con};
bind(Queue, Exchange, [H | T], Con) ->
    {ok, Con1} = zamqp_conn:bind(Queue, Exchange, H, Con),
    bind(Queue, Exchange, T, Con1);
bind(Queue, Exchange, RoutingKey, Con) when is_binary(RoutingKey) ->
    zamqp_conn:bind(Queue, Exchange, RoutingKey, Con).


beat(State = #state{connection = Con}) ->
    zamqp_conn:heartbeat(Con),
    reset_beat(State).

handle_data(Data, State = #state{connection = Con, queue = Queue}) ->
    try handle(Data, State) of
        cancel ->
            close_prejudice(Con),
            ?WARNING("Canceled queue ~p", [Queue]),
            connect(immedately, State);
        close ->
            close_prejudice(Con),
            connect(immedately, State);
        State0 ->
            State0
    catch
        ?WITH_STACKTRACE(Class, Reason, Stacktrace)
            #state{host = Host, port = Port} = State,
            ?ERROR("Failed to handle message ~p:~p:~p ~p:~p trace:~p",
                   [Host,
                    Port,
                    Queue,
                    Class,
                    Reason,
                    Stacktrace]),
            close_prejudice(Con),
            connect(immedately, State)
    end.

handle(Bin, State = #state{module = Mod, type=Type,tag=CTag,connection=Con}) ->
    case zamqp_conn:ingest(Bin, Con) of
        {ok, #{method := cancel}, _} ->
            zamqp_conn:cancel_ok(consumer, CTag, Con),
            cancel;
        {ok, Frame = #{method := close}, _} ->
            #{failing_class := Class, failing_method := Method, text := Text} =
                Frame,
            ?ERROR("Server closed: mod:~p type:~p class:~p method:~p text:~p",
                   [Mod, Type, Class, Method, Text]),
            close;
        {ok, #{frame := heartbeat}, Con1} ->
            is_done(Con1),
            reset_timeout(State#state{connection = Con1});
        {ok,#{delivery_tag := Tag, routing_key := Key, body:=Body}, Con1} ->
            State1 = State#state{connection = Con1},
            case {Type, Body} of
                {pool, Body} -> pool(Key, Body, Tag, <<>>, State1);
                {relay, Body} ->
                    relay(Mod, State#state.arity, Key, Body),
                    zamqp_conn:ack(Tag, Con1);
                {instance, Body} ->
                    relay(Mod, State#state.arity, Key, Body),
                    zamqp_conn:ack(Tag, Con1)
            end,
            is_done(Con1),
            reset_timeout(State1)
    end.

relay(Mod, Arity, Key, Body) ->
    try case Arity of 1 ->  Mod:handle(Body); 2 -> Mod:handle(Key, Body) end of
        ok -> ?INFO("Key: ~p Json: ~p Status: ~p Module: ~p", [Key, Body, ok, Mod]);
        error -> ?ERROR("Key: ~p Json: ~p Status: ~p Module: ~p", [Key, Body, error, Mod])
    catch
        ?WITH_STACKTRACE(Class, Reason, Stacktrace)
            ?ERROR("Failed to handle message ~p ~p:~p:~p for ~p:~p",
                   [Mod, Class, Reason, Stacktrace, Key, Body])
    end.

pool(Key, Body, Tag, Log, State) ->
    #state{module = Mod, queue = Queue, connection = Con} = State,
    zamqp_pool_master:activate({Mod, Queue}, Key, Body, Tag, Log, Con).

is_done(undefined) -> ok;
is_done(Con) ->
    case zamqp_conn:empty(Con) of
        false -> continue();
        true -> zamqp_conn:active_once(Con)
    end.

continue() -> self() ! continue.

start_timers(State = #state{heartbeat = HeartBeat}) ->
    State#state{beat = erlang:send_after(HeartBeat, self(), beat),
                timeout = erlang:send_after(HeartBeat * 2, self(), timeout)}.

stop_timers(State = #state{beat = Beat, timeout = Timeout}) ->
    cancel(Beat),
    cancel(Timeout),
    State#state{beat = undefined, timeout = undefined}.

reset_beat(State = #state{heartbeat = HeartBeat, beat = Ref}) ->
    cancel(Ref),
    State#state{beat = erlang:send_after(HeartBeat, self(), beat)}.

reset_timeout(State = #state{heartbeat = HeartBeat, timeout = Ref}) ->
    cancel(Ref),
    State#state{timeout = erlang:send_after(HeartBeat * 2, self(), timeout)}.

cancel(undefined) -> ok;
cancel(Ref) -> erlang:cancel_timer(Ref).

close(undefined) -> ok;
close(Con) -> zamqp_conn:close(Con).

close_prejudice(undefined) -> ok;
close_prejudice(Con) -> zamqp_conn:close_prejudice(Con).
