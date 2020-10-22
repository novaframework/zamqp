-module(zamqp_publisher_master).

-behaviour(gen_server).

%% API
-export([publish/4]).

%% Members API
-export([enter/1, leave/1]).

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
-define(MAX_QUEUE, 10000).

%% Records
-record(state, {pool :: atom(),
                members = [] :: [pid()],
                current = [] :: [pid()],
                queue = [] :: [pid()]
               }).

%% ===================================================================
%% API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec publish(atom(), binary(), binary(), binary()) -> _.
%%--------------------------------------------------------------------
publish(Exchange, Key, Event, UUID) ->
    gen_server:cast(Exchange, {publish, Key, Event, UUID}).

%% ===================================================================
%% Members API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec enter(atom()) -> ok.
%%--------------------------------------------------------------------
enter(Pool) ->
    link(whereis(Pool)),
    gen_server:cast(Pool, {enter, self()}).

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec leave(atom()) -> ok.
%%--------------------------------------------------------------------
leave(Pool) -> exit(whereis(Pool), left).

%% ===================================================================
%% Management API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok, Pid}
%% @doc
%%   Starts the cowboy controlling server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(atom()) -> {ok, pid()}.
%%--------------------------------------------------------------------
start_link(Pool) -> gen_server:start_link({local, Pool}, ?MODULE, Pool, []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec init(atom()) -> {ok, #state{}}.
%%--------------------------------------------------------------------
init(Pool) ->
    process_flag(trap_exit, true),
    {ok, #state{pool = Pool}}.

%%--------------------------------------------------------------------
-spec handle_call(_, _, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_call(Req, _, State) ->
    ?UNEXPECTED(call, Req),
    {noreply, State}.

%%--------------------------------------------------------------------
-spec handle_cast(_, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_cast({enter, Pid}, State = #state{members = [], queue = Queue}) ->
    [zamqp_publisher:publish(Pid, Key, Event, UUID) ||
        {publish, Key, Event, UUID} <- Queue],
    {noreply, State#state{members = [Pid], queue = []}};
handle_cast({enter, Pid}, State = #state{members = Members}) ->
    {noreply, State#state{members = [Pid | lists:delete(Pid, Members)]}};
handle_cast(Req = {publish, _, _, UUID}, State = #state{current = []}) ->
    #state{members = Members, queue = Queue} = State,
    case {Members, length(Queue) > ?MAX_QUEUE} of
        {[], false} -> {noreply, State#state{queue = [Req | Queue]}};
        {[], true} ->
            ?WARNING("Dropped overlarge queue: ~p ~p",
                  [[element(4, E) || E <- Queue], UUID]),
            {noreply, State#state{queue = []}};
        _ ->
            handle_cast(Req, State#state{current = Members})
    end;
handle_cast({publish, Key, Event, UUID}, State = #state{current = [H | T]}) ->
    zamqp_publisher:publish(H, Key, Event, UUID),
    {noreply, State#state{current = T}};
handle_cast(Cast, State) ->
    ?UNEXPECTED(cast, Cast),
    {noreply, State}.

%%--------------------------------------------------------------------
-spec handle_info(_, #state{}) -> {noreply, #state{}}.
%%--------------------------------------------------------------------
handle_info({'EXIT', Pid, _}, State=#state{members=Members,current=Current}) ->
    {noreply, State#state{members = lists:delete(Pid, Members),
                          current = lists:delete(Pid, Current)}};
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
