-module(zamqp_consumers_sup).

-behaviour(supervisor).

%% Management API
-export([start_link/2]).

%% supervisor callbacks
-export([init/1]).

%% ===================================================================
%% Management API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok, Pid}
%% @doc
%%   Starts the main supervisor of the zamqp application.
%% @end
%%--------------------------------------------------------------------
-spec start_link([#{}], [#{}]) -> {ok, pid()} | ignore | {error, _}.
%%--------------------------------------------------------------------
start_link(Consumers, Servers) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {Consumers, Servers}).

%% ===================================================================
%% supervisor callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec init({[#{}], [#{}]}) ->
          {ok, {supervisor:sup_flags(), [sup_flags:child_spec()]}}.
%%--------------------------------------------------------------------
init({Consumers, Servers}) ->
    {ok, {{one_for_one, 100, 3600}, [child(C, Servers) || C <- Consumers]}}.

%% ===================================================================
%% Internal functions.
%% ===================================================================

child(Consumer, Servers) ->
    Consumer1 = add_arity(Consumer),
    case maps:get(type, Consumer1, undefined) of
        pool -> pool(Consumer1, Servers);
        instance -> instance(Consumer1, Servers);
        relay -> relay(Consumer1, Servers);
        undefined ->
            case maps:get(queue, Consumer1, undefined) of
                undefined -> instance(Consumer1#{type => instance}, Servers);
                _ -> pool(Consumer1#{type => pool}, Servers)
            end
    end.

pool(Consumer = #{module := Mod, queue := Queue}, Servers) ->
    {{pool, Mod, Queue},
     {zamqp_pool_consumer_sup, start_link, [Consumer, Servers]},
     permanent, infinity, supervisor, [zamqp_pool_consumer_sup]}.

instance(Consumer = #{module := Mod}, Servers) ->
    Queue = gen_name(Mod),
    Consumer1 = Consumer#{queue => Queue, auto_delete => true},
    {{instance, Mod, Queue},
     {zamqp_instance_consumer_sup, start_link, [Consumer1, Servers]},
     permanent, infinity, supervisor, [zamqp_pool_sup]}.

relay(Consumer = #{module := Mod, queue := Queue}, Servers) ->
    {{relay, Mod, Queue},
     {zamqp_relay_consumer_sup, start_link, [Consumer, Servers]},
     permanent, infinity, supervisor, [zamqp_relay_consumer_sup]}.

gen_name(Mod) ->
    Rand = << <<(X  + $a)>> || <<X:4>> <= crypto:strong_rand_bytes(4)>>,
    <<(atom_to_binary(Mod, utf8))/binary, $-, Rand/binary>>.

add_arity(Consumer = #{module := Module}) ->
    Consumer#{arity => plist:find(handle, Module:module_info(exports))}.
