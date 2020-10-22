-module(zamqp_pool_master).

-behaviour(gen_server).

%% Consumer API
-export([activate/6]).

%% Pool supervisor API
-export([sup/1]).

%% Management API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3
        ]).

%% Includes
-include_lib("zamqp.hrl").

%% Defines

%% Records
-record(state, {sup :: pid()}).

%% ===================================================================
%% Consumer API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec activate(_, binary(), json:json(), binary(), _, _) -> ok.
%%--------------------------------------------------------------------
activate(Id, Key, JSON, Tag, Log, Con) ->
    case zamqp_reg:lookup(Id) of
        undefined -> ?ERROR("No pool master for ~p", [Id]);
        Master -> gen_server:call(Master, {activate, Key, JSON, Tag, Log, Con})
    end.

%% ===================================================================
%% Worker supervisor API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec sup(_) -> ok.
%%--------------------------------------------------------------------
sup(Id) ->
    case zamqp_reg:lookup(Id) of
        undefined -> ?ERROR("No pool master for ~p", [Id]);
        Master -> gen_server:cast(Master, {sup, self()})
    end.


%% ===================================================================
%% Management API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok, Pid}
%% @doc
%%   Starts the cowboy controlling server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(#{}) -> {ok, pid()}.
%%--------------------------------------------------------------------
start_link(Consumer) -> gen_server:start_link(?MODULE, Consumer, []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec init(#{}) -> {ok, #state{}}.
%%--------------------------------------------------------------------
init(#{module := Mod, queue := Queue}) ->
    process_flag(trap_exit, true),
    zamqp_reg:add({Mod, Queue}),
    {ok, #state{}}.

%%--------------------------------------------------------------------
-spec handle_call(_, _, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_call({activate, Key, JSON, Tag, Log, Con}, _, State = #state{sup=Sup}) ->
    {ok, _} = zamqp_pool_workers_sup:start_child(Sup, Key, JSON, Tag, Log, Con),
    {reply, ok, State};
handle_call(Req, _, State) ->
    ?UNEXPECTED(call, Req),
    {noreply, State}.

%%--------------------------------------------------------------------
-spec handle_cast(_, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_cast({sup, SupPid}, State) -> {noreply, State#state{sup = SupPid}};
handle_cast(Cast, State) ->
    ?UNEXPECTED(cast, Cast),
    {noreply, State}.

%%--------------------------------------------------------------------
-spec handle_info(_, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_info(Info, State) ->
    ?UNEXPECTED(info, Info),
    {noreply, State}.

%%--------------------------------------------------------------------
-spec terminate(_, #state{}) -> ok.
%%--------------------------------------------------------------------
terminate(_, _) -> ok.

%%--------------------------------------------------------------------
-spec code_change(_, #state{}, _) -> {ok, #state{}}.
%%--------------------------------------------------------------------
code_change(_, State, _) -> {ok, State}.

%% ===================================================================
%% Internal functions.
%% ===================================================================
