-module(zamqp_publisher_pool_sup_tests).

%% Includes
-include_lib("eunit/include/eunit.hrl").

%% Defines

%%%-------------------------------------------------------------------
%% gen_server
%%%-------------------------------------------------------------------


gen_server_test_() ->
    {setup,
     fun setup_gen_server/0,
     fun cleanup_gen_server/1,
     [{timeout, 120, {"Start gen_server", ?_test(run_gen_server(start))}}
     ]}.

setup_gen_server() ->
    Servers = [#{user => <<"guest">>,
                 pass => <<"guest">>,
                 host => {127, 0, 0, 1},
                 port => 5672}],
    Publisher = #{exchange => gen_server_test_e},
    register(gen_server_test_e, self()),
    {ok, Pid} = zamqp_publisher_pool_sup:start_link(Servers, Publisher),
    receive
        {'$gen_cast', {enter, _}} -> ok
    after
        1000 -> erlang:error(timeout)
    end,
    register(zamqp_publisher_pool_sup, Pid),
    unregister(gen_server_test_e),
    Pid.

cleanup_gen_server(Pid) ->
    [{_, Pid1, _, _}] = supervisor:which_children(zamqp_publisher_pool_sup),
    unlink(Pid),
    unlink(Pid1),
    unregister(zamqp_publisher_pool_sup),
    exit(Pid, shutdown).

run_gen_server(start) ->
    [{_, Pid, _, _}] = supervisor:which_children(zamqp_publisher_pool_sup),
    sys:get_status(Pid),
    zamqp_publisher:publish(gen_server_test_e,
                            <<"A key">>,
                            <<"A test">>,
                            <<"Test UUID">>),
    sys:get_status(Pid).
