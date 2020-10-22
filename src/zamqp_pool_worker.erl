-module(zamqp_pool_worker).

-behaviour(gen_server).

%% Management API
-export([start_link/6]).

%% gen_server callbacks
-export([init/1,
         handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3
        ]).

%% Includes
-include_lib("zamqp.hrl").

%% Defines

%% Records
-record(state, {key :: binary(),
                json :: json:json(),
                tag :: binary(),
                log :: _,
                con :: _,
                consumer :: #{}
               }).

%% ===================================================================
%% Management API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok, Pid}
%% @doc
%%   Starts the
%% @end
%%--------------------------------------------------------------------
-spec start_link(#{}, binary(), json:json(), binary(), _, _) -> {ok, pid()}.
%%--------------------------------------------------------------------
start_link(Consumer, Key, JSON, Tag, Log, Con) ->
    gen_server:start_link(?MODULE, {Consumer, Key, JSON, Tag, Log, Con}, []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec init({#{}, binary(), json:json(), pid()}) -> {ok, #state{}}.
%%--------------------------------------------------------------------
init({Consumer, Key, JSON, Tag, Log, Con}) ->
    self() ! work,
    {ok,
     #state{key = Key,
            json = JSON,
            tag = Tag,
            log = Log,
            con = Con,
            consumer = Consumer}}.

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
handle_info(work, State) ->
    #state{key = Key,
           json = JSON,
           tag = Tag,
           log = Log,
           con = Con,
           consumer = #{arity := Arity, module := Mod}} = State,
    try
        case Arity of
            1 ->  Mod:handle(JSON);
            2 -> Mod:handle(Key, JSON)
        end
    of
        ok -> ?INFO("Json: ~p Status: ~p Module: ~p", [JSON, ok, Mod]);
        error -> ?ERROR("Json: ~p Status: ~p Module: ~p", [JSON, ok, Mod])
    catch
        ?WITH_STACKTRACE(Class, Reason, Stacktrace)
            ?ERROR("Failed to handle message ~p ~p:~p:~p for ~p:~p",
                   [Mod, Class, Reason, Stacktrace, Key, JSON])
    end,
    zamqp_conn:ack(Tag, Con),
    {stop, normal, State};
handle_info(Info, State) ->
    ?UNEXPECTED(info, Info),
    {noreply, State}.

%%--------------------------------------------------------------------
-spec terminate(_, #state{}) -> ok.
%%--------------------------------------------------------------------
terminate(_, State) -> State.

%%--------------------------------------------------------------------
-spec code_change(_, #state{}, _) -> {ok, #state{}}.
%%--------------------------------------------------------------------
code_change(_, State, _) -> {ok, State}.

%% ===================================================================
%% Internal functions.
%% ===================================================================
