-module(zamqp_publisher_sup_tests).

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
    {ok, Pid} = zamqp_publisher_sup:start_link(Servers, Publisher),
    register(zamqp_publisher_sup, Pid),
    Pid.

cleanup_gen_server(Pid) ->
    unlink(Pid),
    unregister(zamqp_publisher_sup),
    exit(Pid, shutdown).

run_gen_server(start) ->
    [Pid1] =
        [Pid || {zamqp_publisher_pool_sup, Pid, _, _} <-
                    supervisor:which_children(zamqp_publisher_sup)],
    [{_, Pid2, _, _}] = supervisor:which_children(Pid1),
    sys:get_status(Pid2),
    zamqp_publisher:publish(gen_server_test_e,
                            <<"A key">>,
                            <<"A test">>,
                            <<"Test UUID">>),
    sys:get_status(Pid2).
