-module(zamqp_publisher).

-behaviour(gen_server).

%% API
-export([publish/4]).

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
-record(state, {user,
                pass,
                host,
                port,
                exchange,
                connection,
                heartbeat,
                timeout,
                beat}).

%% Types

%% ===================================================================
%% API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec publish(pid(), _, _, _) -> ok.
%%--------------------------------------------------------------------
publish(Pid, Key, Event, UUID) ->
    gen_server:cast(Pid, {publish, Key, Event, UUID}).

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
start_link(Publisher, Server) ->
    gen_server:start_link(?MODULE, {Publisher, Server}, []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec init(#{}) -> {ok, #state{}}.
%%--------------------------------------------------------------------
init({#{exchange := Exchange}, Server}) ->
    #{user := User,
      pass := Pass,
      host := Host,
      port := Port} = Server,
    self() ! connect,
    {ok,
     #state{user = User,
            pass = Pass,
            host = Host,
            port = Port,
            exchange = Exchange}}.

%%--------------------------------------------------------------------
-spec handle_call(_, _, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_call(Req, _, State) ->
    ?UNEXPECTED(call, Req),
    {noreply, State}.

%%--------------------------------------------------------------------
-spec handle_cast(_, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_cast({publish, K, E, U}, State = #state{connection = undefined}) ->
    zamqp_publisher_master:publish(State#state.exchange, K, E, U),
    {noreply, State};
handle_cast({publish, Key, Event, UUID}, State = #state{connection = Con}) ->
    State1 =
        try do_publish(Key, Event, State)
        catch
            Class:Error ->
                #state{host = Host, port = Port, exchange = Exchange} = State,
                ?ERROR("Failed to publish ~p:~p:~p ~p:~p ~p",
                       [Host, Port, Exchange, Class, Error, UUID]),
                close_prejudice(Con),
                State0 = connect(immedately, State),
                zamqp_publisher_master:publish(Exchange, Key, Event, UUID),
                State0
        end,
    {noreply, State1};
handle_cast(Cast, State) ->
    ?UNEXPECTED(cast, Cast),
    {noreply, State}.

%%--------------------------------------------------------------------
-spec handle_info(_, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_info(connect, State = #state{host = Host, exchange = Exchange}) ->
    State1 =
        try do_connect(State) of
            {HeartBeat, Con} ->
                ?INFO("Connected to RabbitMQ host:~p exchange:~p",
                      [Host, Exchange]),
                zamqp_publisher_master:enter(Exchange),
                start_timers(State#state{heartbeat = HeartBeat,
                                         connection = Con})
        catch
            ?WITH_STACKTRACE(Class, Reason, Stacktrace)
                #state{user = User, pass = Pass, port = Port} = State,
                ?ERROR("Failed to connect to RabbitMQ ~p:~p (~p:~p): ~p:~p ~p",
                       [User, Pass, Host, Port, Class, Reason, Stacktrace]),
                connect(?CON_INTERVAL, State)
        end,
    {noreply, State1};
handle_info(beat, State) ->
    State1 =
        try beat(State)
        catch
            ?WITH_STACKTRACE(Class, Reason, Stacktrace)
                #state{host = Host, port = Port, connection = Con} = State,
                zamqp_conn:close_prejudice(Con),
                ?ERROR("Failed to beat ~p:~p: ~p:~p ~p",
                       [Host, Port, Class, Reason, Stacktrace]),
                connect(?CON_INTERVAL, State)
        end,
    {noreply, State1};
handle_info(timeout, State) ->
    #state{connection = Con, host = Host, port = Port} = State,
    ?ERROR("RabbitMQ timeout " "~p:~p", [Host, Port]),
    close_prejudice(Con),
    {noreply, connect(immedately, State)};
handle_info({tcp_close, _}, State = #state{host = Host, port = Port}) ->
    ?ERROR("RabbitMQ TCP close ~p:~p", [Host, Port]),
    {noreply, connect(immedately, State)};
handle_info({tcp_error, Sock, Reason}, State) ->
    #state{host = Host, port = Port} = State,
    try gen_tcp:close(Sock) catch _:_ -> ok end,
    ?ERROR("RabbitMQ TCP error ~p ~p:~p", [Reason, Host, Port]),
    {noreply, connect(immedately, State)};
handle_info({tcp, _, Bin}, State = #state{connection = Con}) ->
    State1 =
        try handle_tcp(Bin, State) of
            cancel ->
                close_prejudice(Con),
                connect(immedately, State);
            State0 ->
                State0
        catch
            Class:Error ->
                #state{host = Host, port = Port} = State,
                ?ERROR("Failed to handle message ~p:~p: ~p:~p",
                       [Host, Port, Class, Error]),
                close_prejudice(Con),
                connect(immedately, State)
        end,
    {noreply, State1};
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

connect(immedately, State = #state{exchange = Exchange}) ->
    zamqp_publisher_master:leave(Exchange),
    self() ! connect,
    stop_timers(State#state{connection = undefined});
connect(Time, State = #state{exchange = Exchange}) ->
    zamqp_publisher_master:leave(Exchange),
    erlang:send_after(Time, self(), connect),
    stop_timers(State#state{connection = undefined}).

do_connect(State) ->
    #state{user = User,
           pass = Pass,
           host = Host,
           port = Port,
           exchange = Exchange} = State,
    Open = #{user => User, pass => Pass, host => Host, port => Port},
    {ok, HeartBeat, Con} = zamqp_conn:open(Open),
    try zamqp_conn:declare(exchange, Exchange, Con) of
        {ok, Con1} ->
            zamqp_conn:active_once(Con1),
            {HeartBeat, Con1}
    catch
        Class:Error ->
            zamqp_conn:close_prejudice(Con),
            erlang:Class(Error)
    end.

beat(State = #state{connection = Con}) ->
    zamqp_conn:heartbeat(Con),
    reset_beat(State).

handle_tcp(Bin, State = #state{connection = Con}) ->
    case zamqp_conn:ingest(Bin, Con) of
        {ok, #{method := cancel}, _} -> cancel;
        {ok, #{frame := heartbeat}, Con1} ->
            zamqp_conn:active_once(Con1),
            reset_timeout(State#state{connection = Con1})
    end.

do_publish(Key, Event, State = #state{exchange = Exchange, connection = Con}) ->
    zamqp_conn:publish(Exchange, Key, Event, Con),
    reset_beat(State).

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
