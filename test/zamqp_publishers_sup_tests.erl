-module(zamqp_publishers_sup_tests).

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
    Publishers = [#{exchange => gen_server_test_e}],
    {ok, Pid} = zamqp_publishers_sup:start_link(Servers, Publishers),
    Pid.

cleanup_gen_server(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown).

run_gen_server(start) ->
    zamqp_publisher:publish(gen_server_test_e,
                            <<"A key">>,
                            <<"A test">>,
                            <<"Test UUID">>).
